/*
 * Copyright 2018-2020 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */

#include "RG.h"
#include "../../util/arr.h"
#include "../../util/strcmp.h"
#include "traverse_order_utils.h"

//------------------------------------------------------------------------------
// Scoring functions
//------------------------------------------------------------------------------

// score expression based on its source and destination labels
// score = src label count + dest label count
int TraverseOrder_LabelsScore(AlgebraicExpression *exp, const QueryGraph *qg) {
	ASSERT(exp != NULL);
	ASSERT(qg  != NULL);

	int         score       =  0;
	const char  *src        =  AlgebraicExpression_Source(exp);
	const char  *dest       =  AlgebraicExpression_Destination(exp);
	QGNode      *src_node   =  QueryGraph_GetNodeByAlias(qg, src);
	QGNode      *dest_node  =  QueryGraph_GetNodeByAlias(qg, dest);

	score += QGNode_LabelCount(src_node);

	// consider 'dest' only if different than 'src'
	if(RG_STRCMP(src, dest) != 0) {
		score += QGNode_LabelCount(dest_node);
	}

	return score;
}

// score expression based on the existence of filters applied to entities
// in the expression, adding additional score for independent filtered entities
int TraverseOrder_FilterExistenceScore(AlgebraicExpression *exp,
									   rax *filtered_entities) {
	ASSERT(exp != NULL);

	if(!filtered_entities) return 0;

	int          score          =  0;
	void         *frequency     =  NULL;  // independent occurrences
	const  char  *src           =  AlgebraicExpression_Source(exp);
	const  char  *dest          =  AlgebraicExpression_Destination(exp);
	const  char  *edge          =  AlgebraicExpression_Edge(exp);

	frequency = raxFind(filtered_entities, (unsigned char *)src, strlen(src));
	if(frequency != raxNotFound) {
		score += 2;
		score += (int64_t)frequency * 2;
	}

	// consider 'dest' only if different than 'src'
	if(RG_STRCMP(src, dest) != 0) {
		frequency = raxFind(filtered_entities, (unsigned char *)dest, strlen(dest));
		if(frequency != raxNotFound) {
			score += 2;
			score += (int64_t)frequency * 2;
		}
	}

	// a filtered edge will increase the expression score
	// but not as much as a filtered node, as filtering an edge requires
	// the expression to be traversed, unlike filters applied to nodes
	if(edge) {
		if(raxFind(filtered_entities, (unsigned char *)edge, strlen(edge))
		   != raxNotFound) {
			score += 1;
		}
	}

	return score;
}

// score expression based on bounded nodes
int TraverseOrder_BoundVariableScore(AlgebraicExpression *exp,
									 rax *bound_vars) {
	ASSERT(exp != NULL);

	if(!bound_vars) return 0;

	int         score       =  0;
	bool        src_bound   =  false;
	bool        dest_bound  =  false;
	const char* src         =  AlgebraicExpression_Source(exp);
	const char* dest        =  AlgebraicExpression_Destination(exp);

	src_bound = raxFind(bound_vars, (unsigned char *)src,
			strlen(src)) != raxNotFound;

	// consider 'dest' only if different than 'src'
	if(RG_STRCMP(src, dest) != 0) {
		dest_bound = raxFind(bound_vars, (unsigned char *)dest,
				strlen(dest)) != raxNotFound;
	}

	if(src_bound)  score += 1;
	if(dest_bound) score += 1;

	return score;
}

// collect independent entities
// and the number of their independent occurrences from a filter tree
// an indpendent entity is an entity that is the single entity in a predicate
// for example: 'n.v = 1', `n` is independent
// unlike 'n.v = m.v' in which `n` and `m` depend on one another
void FilterTree_CollectIndependentEntities
(
	const FT_FilterNode *root, // filter tree root
	rax *entities              // [output] populated with independent frequency
) {
	ASSERT(root != NULL);
	ASSERT(entities != NULL);

	// clone input filter-tree as we're about to modify it
	// breaking it down to sub-trees
	FT_FilterNode  *tree           =  FilterTree_Clone(root);
	FT_FilterNode  **sub_trees     =  FilterTree_SubTrees(tree);
	uint           sub_tree_count  =  array_len(sub_trees);

	// for each sub tree of 'root'
	for(uint i = 0; i < sub_tree_count; i++) {
		// 'n' number of different entities mentioned in 't'
		// if 'n' is 1, 't' relies on just a single graph entity 'e' which makes
		// 'e' independent
		uint n = 0;
		raxIterator it;
		FT_FilterNode *t = sub_trees[i];
		rax *modified = FilterTree_CollectModified(t);

		n = raxSize(modified);
		if(n == 1) {
			raxStart(&it, modified);
			raxSeek(&it, "^", NULL, 0);
			raxNext(&it);

			// set entity frequency
			void *freq = raxFind(entities, it.key, it.key_len);
			ASSERT(freq != raxNotFound);
			freq = (freq == NULL) ? (void *)1 : (void *)(freq + 1);
			raxInsert(entities, it.key, it.key_len, freq, NULL);

			raxStop(&it);
		}

		raxFree(modified);
		FilterTree_Free(t);
	}

	array_free(sub_trees);
}

// score each expression and sort expressions by score
void TraverseOrder_ScoreExpressions
(
	ScoredExp *scored_exps,      // sorted array of scored expressions
	AlgebraicExpression **exps,  // expressions to score
	uint nexp,                   // number of expressions
	rax *bound_vars,             // map of bounded entities
	rax *filtered_entities,      // map of filtered entities
	const QueryGraph *qg         // query graph
) {
	// scoring of algebraic expression is done according to 3 criterias
	// ordered by strongest to weakest:
	// 1. The source or destination are bound
	// 2. Existence of filters on either source or destinaion
	// 3. Label(s) on the expression source or destination
	//
	// the expressions will be evaluated in 3 phases, one for each criteria
	// (from weakest to strongest)
	//
	// the score given for each criteria is:
	// (the maximum score given in the previous criteria) + (criteria scoring function)
	// where the first criteria starts with (criteria scoring function)
	//
	// phase 1 - check for labels on either source or destination
	// expression scoring = _expression_labels_score
	//
	// phase 2 - check for existence of filters on either source or destinaion
	// expression scoring = (max(phase 1 scoring results)) + _expression_filter_existence_score
	//
	// phase 3 - bound variables
	// phase expression scoring = (max(phase 2 scoring results)) +  _expression_bound_variable_score
	//

	int                  max          =  0;
	int                  score        =  0;
	int                  currmax      =  0;
	AlgebraicExpression  *exp         =  NULL;
	ScoredExp            *scored_exp  =  NULL;

	//--------------------------------------------------------------------------
	//  phase 1 score label
	//--------------------------------------------------------------------------

	for(uint i = 0; i < nexp; i ++) {
		exp = exps[i];
		scored_exp = scored_exps + i;

		score = TraverseOrder_LabelsScore(exp, qg);
		scored_exp->exp = exp;
		scored_exp->score = score;

		max = MAX(max, score);
	}

	// update phase 1 maximum score
	currmax = max;

	//--------------------------------------------------------------------------
	//  phase 2 score filters
	//--------------------------------------------------------------------------

	if(filtered_entities) {
		for(uint i = 0; i < nexp; i ++) {
			scored_exp = scored_exps + i;
			exp = scored_exp->exp;

			score = TraverseOrder_FilterExistenceScore(exp, filtered_entities);
			if(score > 0) {
				score += currmax;
				scored_exp->score += score;
				max = MAX(max, scored_exp->score);
			}
		}

		// update phase 2 maximum score
		currmax = max;
	}

	//--------------------------------------------------------------------------
	//  phase 3 score bound variables
	//--------------------------------------------------------------------------

	if(bound_vars) {
		for(uint i = 0; i < nexp; i ++) {
			scored_exp = scored_exps + i;
			exp = scored_exp->exp;

			score = TraverseOrder_BoundVariableScore(exp, bound_vars);
			if(score > 0) {
				score += currmax;
				scored_exp->score += score;
			}
		}
	}
}

