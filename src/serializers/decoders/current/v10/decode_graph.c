/*
* Copyright 2018-2020 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/

#include "decode_v10.h"

// Module event handler functions declarations.
void ModuleEventHandler_IncreaseDecodingGraphsCount(void);
void ModuleEventHandler_DecreaseDecodingGraphsCount(void);

static GraphContext *_GetOrCreateGraphContext
(
	char *graph_name
) {
	GraphContext *gc = GraphContext_GetRegisteredGraphContext(graph_name);
	if(!gc) {
		// New graph is being decoded. Inform the module and create new graph context.
		ModuleEventHandler_IncreaseDecodingGraphsCount();
		gc = GraphContext_New(graph_name, GRAPH_DEFAULT_NODE_CAP, GRAPH_DEFAULT_EDGE_CAP);
		// While loading the graph, minimize matrix realloc and synchronization calls.
		Graph_SetMatrixPolicy(gc->g, SYNC_POLICY_RESIZE);
	}
	// Free the name string, as it either not in used or copied.
	RedisModule_Free(graph_name);

	// Set the GraphCtx in thread-local storage.
	QueryCtx_SetGraphCtx(gc);

	return gc;
}

// the first initialization of the graph data structure guarantees that
// there will be no further re-allocation of data blocks and matrices
// since they are all in the appropriate size
static void _InitGraphDataStructure
(
	Graph *g,
	uint64_t node_count,
	uint64_t edge_count,
	uint64_t label_count,
	uint64_t relation_count
) {
	DataBlock_Accommodate(g->nodes, node_count);
	DataBlock_Accommodate(g->edges, edge_count);
	for(uint64_t i = 0; i < label_count; i++) Graph_AddLabel(g);
	for(uint64_t i = 0; i < relation_count; i++) Graph_AddRelationType(g);
	// flush all matrices
	// guarantee matrix dimensions matches graph's nodes count
	Graph_ApplyAllPending(g, true);
}

static GraphContext *_DecodeHeader
(
	RedisModuleIO *rdb
) {
	// Header format:
	// Graph name
	// Node count
	// Edge count
	// Label matrix count
	// Relation matrix count - N
	// Does relationship matrix Ri holds mutiple edges under a single entry X N
	// Number of graph keys (graph context key + meta keys)

	// graph name
	char *graph_name = RedisModule_LoadStringBuffer(rdb, NULL);

	// each key header contains the following:
	// #nodes, #edges, #labels matrices, #relation matrices
	uint64_t  node_count      =  RedisModule_LoadUnsigned(rdb);
	uint64_t  edge_count      =  RedisModule_LoadUnsigned(rdb);
	uint64_t  label_count     =  RedisModule_LoadUnsigned(rdb);
	uint64_t  relation_count  =  RedisModule_LoadUnsigned(rdb);
	uint64_t  multi_edge[relation_count];

	for(uint i = 0; i < relation_count; i++) {
		multi_edge[i] = RedisModule_LoadUnsigned(rdb);
	}

	// total keys representing the graph
	uint64_t key_number = RedisModule_LoadUnsigned(rdb);

	GraphContext *gc = _GetOrCreateGraphContext(graph_name);
	Graph *g = gc->g;

	// if it is the first key of this graph,
	// allocate all the data structures, with the appropriate dimensions
	if(GraphDecodeContext_GetProcessedKeyCount(gc->decoding_context) == 0) {
		_InitGraphDataStructure(gc->g, node_count, edge_count, label_count, relation_count);

		gc->decoding_context->multi_edge = array_new(uint64_t, relation_count);
		for(uint i = 0; i < relation_count; i++) {
			// enable/Disable support for multi-edge
			// we will enable support for multi-edge on all relationship
			// matrices once we finish loading the graph
			array_append(gc->decoding_context->multi_edge,  multi_edge[i]);
		}

		GraphDecodeContext_SetKeyCount(gc->decoding_context, key_number);
	}

	return gc;
}

static PayloadInfo *_RdbLoadKeySchema
(
	RedisModuleIO *rdb
) {
	// Format:
	// #Number of payloads info - N
	// N * Payload info:
	//     Encode state
	//     Number of entities encoded in this state.

	uint64_t payloads_count = RedisModule_LoadUnsigned(rdb);
	PayloadInfo *payloads = array_new(PayloadInfo, payloads_count);

	for(uint i = 0; i < payloads_count; i++) {
		// for each payload
		// load its type and the number of entities it contains
		PayloadInfo payload_info;
		payload_info.state =  RedisModule_LoadUnsigned(rdb);
		payload_info.entities_count =  RedisModule_LoadUnsigned(rdb);
		array_append(payloads, payload_info);
	}
	return payloads;
}

GraphContext *RdbLoadGraphContext_v10
(
	RedisModuleIO *rdb
) {

	// Key format:
	//  Header
	//  Payload(s) count: N
	//  Key content X N:
	//      Payload type (Nodes / Edges / Deleted nodes/ Deleted edges/ Graph schema)
	//      Entities in payload
	//  Payload(s) X N

	GraphContext *gc = _DecodeHeader(rdb);

	// load the key schema
	PayloadInfo *key_schema = _RdbLoadKeySchema(rdb);

	// The decode process contains the decode operation of many meta keys, representing independent parts of the graph
	// Each key contains data on one or more of the following:
	// 1. Nodes - The nodes that are currently valid in the graph
	// 2. Deleted nodes - Nodes that were deleted and there ids can be re-used. Used for exact replication of data block state
	// 3. Edges - The edges that are currently valid in the graph
	// 4. Deleted edges - Edges that were deleted and there ids can be re-used. Used for exact replication of data block state
	// 5. Graph schema - Properties, indices
	// The following switch checks which part of the graph the current key holds, and decodes it accordingly
	uint payloads_count = array_len(key_schema);
	for(uint i = 0; i < payloads_count; i++) {
		PayloadInfo payload = key_schema[i];
		switch(payload.state) {
			case ENCODE_STATE_NODES:
				Graph_SetMatrixPolicy(gc->g, SYNC_POLICY_NOP);
				RdbLoadNodes_v10(rdb, gc, payload.entities_count);
				break;
			case ENCODE_STATE_DELETED_NODES:
				RdbLoadDeletedNodes_v10(rdb, gc, payload.entities_count);
				break;
			case ENCODE_STATE_EDGES:
				Graph_SetMatrixPolicy(gc->g, SYNC_POLICY_NOP);
				RdbLoadEdges_v10(rdb, gc, payload.entities_count);
				break;
			case ENCODE_STATE_DELETED_EDGES:
				RdbLoadDeletedEdges_v10(rdb, gc, payload.entities_count);
				break;
			case ENCODE_STATE_GRAPH_SCHEMA:
				RdbLoadGraphSchema_v10(rdb, gc);
				break;
			default:
				ASSERT(false && "Unknown encoding");
				break;
		}
	}
	array_free(key_schema);

	// update decode context
	GraphDecodeContext_IncreaseProcessedKeyCount(gc->decoding_context);

	// before finalizing keep encountered meta keys names, for future deletion
	const RedisModuleString *rm_key_name = RedisModule_GetKeyNameFromIO(rdb);
	const char *key_name = RedisModule_StringPtrLen(rm_key_name, NULL);

	// the virtual key name is not equal the graph name
	if(strcmp(key_name, gc->graph_name) != 0) {
		GraphDecodeContext_AddMetaKey(gc->decoding_context, key_name);
	}

	if(GraphDecodeContext_Finished(gc->decoding_context)) {
		Graph *g = gc->g;

		// revert to default synchronization behavior
		Graph_SetMatrixPolicy(g, SYNC_POLICY_FLUSH_RESIZE);
		Graph_ApplyAllPending(g, true);

		// set the thread-local GraphContext
		// as it will be accessed when creating indexes
		QueryCtx_SetGraphCtx(gc);
		
		uint label_count = Graph_LabelTypeCount(g);
		// update the node statistics
		// index the nodes
		for(uint i = 0; i < label_count; i++) {
			GrB_Index nvals;
			RG_Matrix L = Graph_GetLabelMatrix(g, i);
			RG_Matrix_nvals(&nvals, L);
			GraphStatistics_IncNodeCount(&g->stats, i, nvals);

			Schema *s = GraphContext_GetSchemaByID(gc, i, SCHEMA_NODE);
			if(s->index) Index_Construct(s->index);
			if(s->fulltextIdx) Index_Construct(s->fulltextIdx);
		}

		// make sure graph doesn't contains may pending changes
		ASSERT(Graph_Pending(g) == false);

		QueryCtx_Free(); // release thread-local variables
		GraphDecodeContext_Reset(gc->decoding_context);

		// graph has finished decoding, inform the module
		ModuleEventHandler_DecreaseDecodingGraphsCount();
		RedisModuleCtx *ctx = RedisModule_GetContextFromIO(rdb);
		RedisModule_Log(ctx, "notice", "Done decoding graph %s", gc->graph_name);
	}
	return gc;
}
