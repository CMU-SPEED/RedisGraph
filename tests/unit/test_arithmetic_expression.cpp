/*
* Copyright 2018-2019 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/

#include "../../deps/googletest/include/gtest/gtest.h"

#ifdef __cplusplus
extern "C"
{
#endif

#include "../../src/value.h"
#include "../../src/arithmetic/arithmetic_expression.h"
#include "../../src/graph/entities/node.h"
#include "../../src/arithmetic/agg_funcs.h"
#include "../../src/execution_plan/record.h"
#include "../../src/execution_plan/execution_plan.h"
#include "../../src/util/rmalloc.h"
#include "../../src/util/arr.h"
#include <time.h>

// Declaration of function in execution_plan.h
AR_ExpNode** _BuildReturnExpressions(RecordMap *record_map, const cypher_astnode_t *ret_clause);

#ifdef __cplusplus
}
#endif

pthread_key_t _tlsASTKey;  // Thread local storage AST key.

class ArithmeticTest: public ::testing::Test {
  protected:
    static void SetUpTestCase() {
      // Use the malloc family for allocations
      Alloc_Reset();

      AR_RegisterFuncs();
      Agg_RegisterFuncs();

      int error = pthread_key_create(&_tlsASTKey, NULL);
      ASSERT_EQ(error, 0);
    }

    static void TearDownTestCase()
    {
    }
};

void _test_string(const AR_ExpNode *exp, const char *expected)
{
    char *str;
    AR_EXP_ToString(exp, &str);
    ASSERT_STREQ(str, expected);
    free(str);
}

void _test_ar_func(AR_ExpNode *root, SIValue expected, const Record r)
{
    SIValue res = AR_EXP_Evaluate(root, r);
    if (SI_TYPE(res) == T_NULL && SI_TYPE(expected) == T_NULL)
    {
        // NULLs implicitly match
        return;
    }
    else if (SI_TYPE(res) & SI_NUMERIC && SI_TYPE(expected) & SI_NUMERIC)
    {
        // Compare numerics by internal value
        ASSERT_EQ(SI_GET_NUMERIC(res), SI_GET_NUMERIC(expected));
    }
    else
    {
        FAIL() << "Tried to compare disjoint types";
    }
}

AR_ExpNode* _exp_from_query(const char *query) {
  cypher_parse_result_t *parse_result = cypher_parse(query, NULL, NULL, CYPHER_PARSE_ONLY_STATEMENTS);
  AST *ast = AST_Build(parse_result);
  ast->entity_map = NewTrieMap();

  const cypher_astnode_t *ret_clause = AST_GetClause(ast, CYPHER_AST_RETURN);
  AR_ExpNode **return_elems = _BuildReturnExpressions(NULL, ret_clause);

  return return_elems[0];
}

TEST_F(ArithmeticTest, ExpressionTest)
{
    SIValue result;
    const char *query;
    AR_ExpNode *arExp;
    Record r = Record_New(0);

    /* muchacho */
    query = "RETURN 'muchacho'";
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, r);
    ASSERT_STREQ(result.stringval, "muchacho");
    AR_EXP_Free(arExp);

    /* 1 */
    query = "RETURN 1";
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, r);
    AR_EXP_Free(arExp);
    ASSERT_EQ(result.longval, 1);

    /* 1+2*3 */
    query = "RETURN 1+2*3";
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, r);
    AR_EXP_Free(arExp);
    ASSERT_EQ(result.longval, 7);

    /* 1 + 1 + 1 + 1 + 1 + 1 */
    query = "RETURN 1 + 1 + 1 + 1 + 1 + 1";
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, r);
    AR_EXP_Free(arExp);
    ASSERT_EQ(result.longval, 6);

    /* ABS(-5 + 2 * 1) */
    query = "RETURN ABS(-5 + 2 * 1)";
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, r);
    AR_EXP_Free(arExp);
    ASSERT_EQ(result.longval, 3);

    /* 'a' + 'b' */
    query = "RETURN 'a' + 'b'";
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, r);
    AR_EXP_Free(arExp);
    ASSERT_TRUE(strcmp(result.stringval, "ab") == 0);

    /* 1 + 2 + 'a' + 2 + 1 */
    query = "RETURN 1 + 2 + 'a' + 2 + 1";
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, r);
    AR_EXP_Free(arExp);
    ASSERT_TRUE(strcmp(result.stringval, "3a21") == 0);

    /* 2 * 2 + 'a' + 3 * 3 */
    query = "RETURN 2 * 2 + 'a' + 3 * 3";
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, r);
    AR_EXP_Free(arExp);
    ASSERT_TRUE(strcmp(result.stringval, "4a9") == 0);
}

TEST_F(ArithmeticTest, NullArithmetic)
{
    SIValue result;
    const char *query;
    AR_ExpNode *arExp;
    Record r = Record_New(0);

    /* null + 1 */
    query = "RETURN null + 1";
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, r);
    ASSERT_TRUE(SIValue_IsNull(result));
    AR_EXP_Free(arExp);

    /* 1 + null */
    query = "RETURN 1 + null";
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, r);
    ASSERT_TRUE(SIValue_IsNull(result));
    AR_EXP_Free(arExp);

    /* null - 1 */
    query = "RETURN null - 1";
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, r);
    ASSERT_TRUE(SIValue_IsNull(result));
    AR_EXP_Free(arExp);

    /* 1 - null */
    query = "RETURN 1 - null";
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, r);
    ASSERT_TRUE(SIValue_IsNull(result));
    AR_EXP_Free(arExp);

    /* null * 1 */
    query = "RETURN null * 1";
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, r);
    ASSERT_TRUE(SIValue_IsNull(result));
    AR_EXP_Free(arExp);

    /* 1 * null */
    query = "RETURN 1 * null";
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, r);
    ASSERT_TRUE(SIValue_IsNull(result));
    AR_EXP_Free(arExp);

    /* null / 1 */
    query = "RETURN null / 1";
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, r);
    ASSERT_TRUE(SIValue_IsNull(result));
    AR_EXP_Free(arExp);

    /* 1 / null */
    query = "RETURN 1 / null";
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, r);
    ASSERT_TRUE(SIValue_IsNull(result));
    AR_EXP_Free(arExp);
}

TEST_F(ArithmeticTest, AggregateTest)
{
    SIValue result;
    const char *query;
    AR_ExpNode *arExp;
    Record r = Record_New(0);

    /* SUM(1) */
    query = "RETURN SUM(1)";
    arExp = _exp_from_query(query);

    AR_EXP_Aggregate(arExp, r);
    AR_EXP_Aggregate(arExp, r);
    AR_EXP_Aggregate(arExp, r);
    AR_EXP_Reduce(arExp);
    result = AR_EXP_Evaluate(arExp, r);
    AR_EXP_Free(arExp);
    ASSERT_EQ(result.doubleval, 3);

    /* 2+SUM(1) */
    query = "RETURN 2+SUM(1)";
    arExp = _exp_from_query(query);
    AR_EXP_Aggregate(arExp, r);
    AR_EXP_Aggregate(arExp, r);
    AR_EXP_Aggregate(arExp, r);
    AR_EXP_Reduce(arExp);
    /* Just for the kick of it, call reduce more than once.*/
    AR_EXP_Reduce(arExp);

    result = AR_EXP_Evaluate(arExp, r);
    AR_EXP_Free(arExp);

    ASSERT_EQ(result.doubleval, 5);
}

TEST_F(ArithmeticTest, AbsTest)
{
    const char *query;
    AR_ExpNode *arExp;
    Record r = Record_New(0);

    /* ABS(1) */
    query = "RETURN ABS(1)";
    arExp = _exp_from_query(query);
    SIValue expected = SI_LongVal(1);
    _test_ar_func(arExp, expected, r);
    AR_EXP_Free(arExp);

    /* ABS(-1) */
    query = "RETURN ABS(-1)";
    arExp = _exp_from_query(query);
    expected = SI_LongVal(1);
    _test_ar_func(arExp, expected, r);
    AR_EXP_Free(arExp);

    /* ABS(0) */
    query = "RETURN ABS(0)";
    arExp = _exp_from_query(query);
    expected = SI_LongVal(0);
    _test_ar_func(arExp, expected, r);
    AR_EXP_Free(arExp);

    /* ABS() */
    query = "RETURN ABS(NULL)";
    arExp = _exp_from_query(query);
    expected = SI_NullVal();
    _test_ar_func(arExp, expected, r);
    AR_EXP_Free(arExp);
}

TEST_F(ArithmeticTest, CeilTest)
{
    SIValue expected;
    const char *query;
    AR_ExpNode *arExp;
    Record r = Record_New(0);

    /* CEIL(0.5) */
    query = "RETURN CEIL(0.5)";
    arExp = _exp_from_query(query);
    expected = SI_DoubleVal(1);
    _test_ar_func(arExp, expected, r);
    AR_EXP_Free(arExp);

    /* CEIL(1) */
    query = "RETURN CEIL(1)";
    arExp = _exp_from_query(query);
    expected = SI_DoubleVal(1);
    _test_ar_func(arExp, expected, r);
    AR_EXP_Free(arExp);

    /* CEIL(0.1) */
    query = "RETURN CEIL(0.1)";
    arExp = _exp_from_query(query);
    expected = SI_DoubleVal(1);
    _test_ar_func(arExp, expected, r);
    AR_EXP_Free(arExp);

    /* CEIL() */
    query = "RETURN CEIL(NULL)";
    arExp = _exp_from_query(query);
    expected = SI_NullVal();
    _test_ar_func(arExp, expected, r);
    AR_EXP_Free(arExp);
}

TEST_F(ArithmeticTest, FloorTest)
{
    SIValue expected;
    const char *query;
    AR_ExpNode *arExp;
    Record r = Record_New(0);

    /* FLOOR(0.5) */
    query = "RETURN FLOOR(0.5)";
    arExp = _exp_from_query(query);
    expected = SI_DoubleVal(0);
    _test_ar_func(arExp, expected, r);
    AR_EXP_Free(arExp);

    /* FLOOR(1) */
    query = "RETURN FLOOR(1)";
    arExp = _exp_from_query(query);
    expected = SI_DoubleVal(1);
    _test_ar_func(arExp, expected, r);
    AR_EXP_Free(arExp);

    /* FLOOR(0.1) */
    query = "RETURN FLOOR(0.1)";
    arExp = _exp_from_query(query);
    expected = SI_DoubleVal(0);
    _test_ar_func(arExp, expected, r);
    AR_EXP_Free(arExp);

    /* FLOOR() */
    query = "RETURN FLOOR(NULL)";
    arExp = _exp_from_query(query);
    expected = SI_NullVal();
    _test_ar_func(arExp, expected, r);
    AR_EXP_Free(arExp);
}

TEST_F(ArithmeticTest, RoundTest)
{
    SIValue expected;
    const char *query;
    AR_ExpNode *arExp;
    Record r = Record_New(0);

    /* ROUND(0) */
    query = "RETURN ROUND(0)";
    arExp = _exp_from_query(query);
    expected = SI_DoubleVal(0);
    _test_ar_func(arExp, expected, r);
    AR_EXP_Free(arExp);

    /* ROUND(0.49) */
    query = "RETURN ROUND(0.49)";
    arExp = _exp_from_query(query);
    expected = SI_DoubleVal(0);
    _test_ar_func(arExp, expected, r);
    AR_EXP_Free(arExp);

    /* ROUND(0.5) */
    query = "RETURN ROUND(0.5)";
    arExp = _exp_from_query(query);
    expected = SI_DoubleVal(1);
    _test_ar_func(arExp, expected, r);
    AR_EXP_Free(arExp);

    /* ROUND(1) */
    query = "RETURN ROUND(1)";
    arExp = _exp_from_query(query);
    expected = SI_DoubleVal(1);
    _test_ar_func(arExp, expected, r);
    AR_EXP_Free(arExp);

    /* ROUND() */
    query = "RETURN ROUND(NULL)";
    arExp = _exp_from_query(query);
    expected = SI_NullVal();
    _test_ar_func(arExp, expected, r);
    AR_EXP_Free(arExp);
}

TEST_F(ArithmeticTest, SignTest)
{
    SIValue expected;
    const char *query;
    AR_ExpNode *arExp;
    Record r = Record_New(0);

    /* SIGN(0) */
    query = "RETURN SIGN(0)";
    arExp = _exp_from_query(query);
    expected = SI_LongVal(0);
    _test_ar_func(arExp, expected, r);
    AR_EXP_Free(arExp);

    /* SIGN(-1) */
    query = "RETURN SIGN(-1)";
    arExp = _exp_from_query(query);
    expected = SI_LongVal(-1);
    _test_ar_func(arExp, expected, r);
    AR_EXP_Free(arExp);

    /* SIGN(1) */
    query = "RETURN SIGN(1)";
    arExp = _exp_from_query(query);
    expected = SI_LongVal(1);
    _test_ar_func(arExp, expected, r);
    AR_EXP_Free(arExp);

    /* SIGN() */
    query = "RETURN SIGN(NULL)";
    arExp = _exp_from_query(query);
    expected = SI_NullVal();
    _test_ar_func(arExp, expected, r);
    AR_EXP_Free(arExp);
}

TEST_F(ArithmeticTest, ReverseTest)
{
    SIValue result;
    const char *expected;
    const char *query;
    AR_ExpNode *arExp;
    Record r = Record_New(0);

    /* REVERSE("muchacho") */
    query = "RETURN REVERSE('muchacho')";
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, r);
    AR_EXP_Free(arExp);
    expected = "ohcahcum";
    ASSERT_STREQ(result.stringval, expected);

    /* REVERSE("") */
    query = "RETURN REVERSE('')";
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, r);
    AR_EXP_Free(arExp);
    expected = "";
    ASSERT_STREQ(result.stringval, expected);

    /* REVERSE() */
    query = "RETURN REVERSE(NULL)";
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, r);
    AR_EXP_Free(arExp);
    ASSERT_EQ(result.type, T_NULL);
}

TEST_F(ArithmeticTest, LeftTest)
{
    SIValue result;
    const char *expected;
    const char *query;
    AR_ExpNode *arExp;
    Record r = Record_New(0);

    /* LEFT("muchacho", 4) */
    query = "RETURN LEFT('muchacho', 4)";
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, r);
    AR_EXP_Free(arExp);
    expected = "much";
    ASSERT_STREQ(result.stringval, expected);

    /* LEFT("muchacho", 100) */
    query = "RETURN LEFT('muchacho', 100)";
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, r);
    AR_EXP_Free(arExp);
    expected = "muchacho";
    ASSERT_STREQ(result.stringval, expected);

    /* LEFT(NULL, 100) */
    query = "RETURN LEFT(NULL, 100)";
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, r);
    AR_EXP_Free(arExp);
    ASSERT_EQ(result.type, T_NULL);
}

TEST_F(ArithmeticTest, RightTest)
{
    SIValue result;
    const char *expected;
    const char *query;
    AR_ExpNode *arExp;
    Record r = Record_New(0);

    /* RIGHT("muchacho", 4) */
    query = "RETURN RIGHT('muchacho', 4)";
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, r);
    AR_EXP_Free(arExp);
    expected = "acho";
    ASSERT_STREQ(result.stringval, expected);

    /* RIGHT("muchacho", 100) */
    query = "RETURN RIGHT('muchacho', 100)";
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, r);
    AR_EXP_Free(arExp);
    expected = "muchacho";
    ASSERT_STREQ(result.stringval, expected);

    /* RIGHT(NULL, 100) */
    query = "RETURN RIGHT(NULL, 100)";
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, r);
    AR_EXP_Free(arExp);
    ASSERT_EQ(result.type, T_NULL);
}

TEST_F(ArithmeticTest, LTrimTest)
{
    SIValue result;
    const char *expected;
    const char *query;
    AR_ExpNode *arExp;
    Record r = Record_New(0);

    /* lTrim("   muchacho") */
    query = "RETURN lTrim('   muchacho')";
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, r);
    AR_EXP_Free(arExp);
    expected = "muchacho";
    ASSERT_STREQ(result.stringval, expected);

    /* lTrim("muchacho   ") */
    query = "RETURN lTrim('muchacho   ')";
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, r);
    AR_EXP_Free(arExp);
    expected = "muchacho   ";
    ASSERT_STREQ(result.stringval, expected);

    /* lTrim("   much   acho   ") */
    query = "RETURN lTrim('   much   acho   ')";
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, r);
    AR_EXP_Free(arExp);
    expected = "much   acho   ";
    ASSERT_STREQ(result.stringval, expected);

    /* lTrim("muchacho") */
    query = "RETURN lTrim('muchacho')";
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, r);
    AR_EXP_Free(arExp);
    expected = "muchacho";
    ASSERT_STREQ(result.stringval, expected);

    /* lTrim() */
    query = "RETURN lTrim(NULL)";
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, r);
    AR_EXP_Free(arExp);
    ASSERT_EQ(result.type, T_NULL);
}

TEST_F(ArithmeticTest, RTrimTest)
{
    SIValue result;
    const char *expected;
    const char *query;
    AR_ExpNode *arExp;
    Record r = Record_New(0);

    /* rTrim("   muchacho") */
    query = "RETURN rTrim('   muchacho')";
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, r);
    AR_EXP_Free(arExp);
    expected = "   muchacho";
    ASSERT_STREQ(result.stringval, expected);

    /* rTrim("muchacho   ") */
    query = "RETURN rTrim('muchacho   ')";
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, r);
    AR_EXP_Free(arExp);
    expected = "muchacho";
    ASSERT_STREQ(result.stringval, expected);

    /* rTrim("   much   acho   ") */
    query = "RETURN rTrim('   much   acho   ')";
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, r);
    AR_EXP_Free(arExp);
    expected = "   much   acho";
    ASSERT_STREQ(result.stringval, expected);

    /* rTrim("muchacho") */
    query = "RETURN rTrim('muchacho')";
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, r);
    AR_EXP_Free(arExp);
    expected = "muchacho";
    ASSERT_STREQ(result.stringval, expected);

    /* rTrim() */
    query = "RETURN rTrim(NULL)";
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, r);
    AR_EXP_Free(arExp);
    ASSERT_EQ(result.type, T_NULL);
}

TEST_F(ArithmeticTest, TrimTest)
{
    SIValue result;
    const char *expected;
    const char *query;
    AR_ExpNode *arExp;
    Record r = Record_New(0);

    /* trim("   muchacho") */
    query = "RETURN trim('   muchacho')";
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, r);
    AR_EXP_Free(arExp);
    expected = "muchacho";
    ASSERT_STREQ(result.stringval, expected);

    /* trim("muchacho   ") */
    query = "RETURN trim('muchacho   ')";
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, r);
    AR_EXP_Free(arExp);
    expected = "muchacho";
    ASSERT_STREQ(result.stringval, expected);

    /* trim("   much   acho   ") */
    query = "RETURN trim('   much   acho   ')";
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, r);
    AR_EXP_Free(arExp);
    expected = "much   acho";
    ASSERT_STREQ(result.stringval, expected);

    /* trim("muchacho") */
    query = "RETURN trim('muchacho')";
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, r);
    AR_EXP_Free(arExp);
    expected = "muchacho";
    ASSERT_STREQ(result.stringval, expected);

    /* trim() */
    query = "RETURN trim(NULL)";
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, r);
    AR_EXP_Free(arExp);
    ASSERT_EQ(result.type, T_NULL);
}

TEST_F(ArithmeticTest, SubstringTest)
{
    SIValue result;
    const char *expected;
    const char *query;
    AR_ExpNode *arExp;
    Record r = Record_New(0);

    /* SUBSTRING("muchacho", 0, 4) */
    query = "RETURN SUBSTRING('muchacho', 0, 4)";
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, r);
    AR_EXP_Free(arExp);
    expected = "much";
    ASSERT_STREQ(result.stringval, expected);

    /* SUBSTRING("muchacho", 3, 20) */
    query = "RETURN SUBSTRING('muchacho', 3, 20)";
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, r);
    AR_EXP_Free(arExp);
    expected = "hacho";
    ASSERT_STREQ(result.stringval, expected);

    /* SUBSTRING(NULL, 3, 20) */
    query = "RETURN SUBSTRING(NULL, 3, 20)";
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, r);
    AR_EXP_Free(arExp);
    ASSERT_EQ(result.type, T_NULL);
}

TEST_F(ArithmeticTest, ToLowerTest)
{
    SIValue result;
    const char *expected;
    const char *query;
    AR_ExpNode *arExp;
    Record r = Record_New(0);

    /* toLower("MuChAcHo") */
    query = "RETURN toLower('MuChAcHo')";
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, r);
    AR_EXP_Free(arExp);
    expected = "muchacho";
    ASSERT_STREQ(result.stringval, expected);

    /* toLower("mUcHaChO") */
    query = "RETURN toLower('mUcHaChO')";
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, r);
    AR_EXP_Free(arExp);
    expected = "muchacho";
    ASSERT_STREQ(result.stringval, expected);

    /* toLower("mUcHaChO") */
    query = "RETURN toLower(NULL)";
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, r);
    AR_EXP_Free(arExp);
    ASSERT_EQ(result.type, T_NULL);
}

TEST_F(ArithmeticTest, ToUpperTest)
{
    SIValue result;
    const char *expected;
    const char *query;
    AR_ExpNode *arExp;
    Record r = Record_New(0);

    /* toUpper("MuChAcHo") */
    query = "RETURN toUpper('MuChAcHo')";
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, r);
    AR_EXP_Free(arExp);
    expected = "MUCHACHO";
    ASSERT_STREQ(result.stringval, expected);

    /* toUpper("mUcHaChO") */
    query = "RETURN toUpper('mUcHaChO')";
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, r);
    AR_EXP_Free(arExp);
    expected = "MUCHACHO";
    ASSERT_STREQ(result.stringval, expected);

    /* toUpper("mUcHaChO") */
    query = "RETURN toUpper(NULL)";
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, r);
    AR_EXP_Free(arExp);
    ASSERT_EQ(result.type, T_NULL);
}

TEST_F(ArithmeticTest, ToStringTest)
{
    SIValue result;
    const char *expected;
    const char *query;
    AR_ExpNode *arExp;
    Record r = Record_New(0);

    /* toString("muchacho") */
    query = "RETURN toString('muchacho')";
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, r);
    AR_EXP_Free(arExp);
    expected = "muchacho";
    ASSERT_STREQ(result.stringval, expected);

    /* toString("3.14") */
    query = "RETURN toString(3.14)";
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, r);
    AR_EXP_Free(arExp);
    expected = "3.140000";
    ASSERT_STREQ(result.stringval, expected);

    /* toString() */
    query = "RETURN toString(NULL)";
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, r);
    AR_EXP_Free(arExp);
    ASSERT_EQ(result.type, T_NULL);
}

TEST_F(ArithmeticTest, ExistsTest)
{
    /* Although EXISTS is supposed to be called 
   * using entity alias and property, to make things easy 
   * within a unit-test context we simply pass an evaluation
   * of an expression such as n.v, `null` when property `v`
   * isn't in `n` and `1` when n.v evaluates to 1. */

    SIValue result;
    const char *query;
    AR_ExpNode *arExp;
    Record r = Record_New(0);

    /* Pass NULL indicating n.v doesn't exists. */
    query = "RETURN EXISTS(null)";
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, r);
    ASSERT_EQ(result.type, T_BOOL);
    ASSERT_EQ(result.longval, 0);
    AR_EXP_Free(arExp);

    /* Pass 1, in case n.v exists and evaluates to 1. */
    query = "RETURN EXISTS(1)";
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, r);
    ASSERT_EQ(result.type, T_BOOL);
    ASSERT_EQ(result.longval, 1);
    AR_EXP_Free(arExp);
}

TEST_F(ArithmeticTest, TimestampTest)
{
    SIValue result;
    const char *query;
    AR_ExpNode *arExp;
    Record r = Record_New(0);

    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);

    query = "RETURN timestamp()";
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, r);

    ASSERT_LE(abs((1000 * ts.tv_sec + ts.tv_nsec / 1000000) - result.longval), 5);
}

TEST_F(ArithmeticTest, CaseTest)
{
    SIValue result;
    SIValue expected = SI_LongVal(2);
    const char *query;
    AR_ExpNode *arExp;

    /* Test "Simple form"
     * Match one of the alternatives. */
    query = "RETURN CASE 'brown' WHEN 'blue' THEN 1+0 WHEN 'brown' THEN 2-0 ELSE 3*1 END";
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, NULL);
    AR_EXP_Free(arExp);
    ASSERT_EQ(result.longval, expected.longval);

    /* Do not match any of the alternatives, return default. */
    query = "RETURN CASE 'green' WHEN 'blue' THEN 1+0 WHEN 'brown' THEN 2-0 ELSE 3*1 END";
    expected = SI_LongVal(3);
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, NULL);
    AR_EXP_Free(arExp);
    ASSERT_EQ(result.longval, expected.longval);

     /* Test "Generic form"
     * One of the alternatives evaluates to a none null value. 
     * Default not specified. */
    query = "RETURN CASE WHEN NULL THEN 1+0 WHEN true THEN 2-0 END";
    expected = SI_LongVal(2);
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, NULL);
    AR_EXP_Free(arExp);    
    ASSERT_EQ(result.longval, expected.longval);

    /* None of the alternatives evaluates to a none null value. 
     * Default specified, expecting default. */
    query = "RETURN CASE WHEN NULL THEN 1+0 WHEN NULL THEN 2-0 ELSE 3*1 END";
    expected = SI_LongVal(3);
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, NULL);
    AR_EXP_Free(arExp);    
    ASSERT_EQ(result.longval, expected.longval);

    /* None of the alternatives evaluates to a none null value. 
     * Default not specified, expecting NULL */
    query = "RETURN CASE WHEN NULL THEN 1+0 WHEN NULL THEN 2-0 END";
    arExp = _exp_from_query(query);
    result = AR_EXP_Evaluate(arExp, NULL);
    AR_EXP_Free(arExp);    
    ASSERT_TRUE(SIValue_IsNull(result));
}

TEST_F(ArithmeticTest, AND)
{
    SIValue truth_table [27] = {
        SI_ConstStringVal("false"), SI_ConstStringVal("false"), SI_BoolVal(false),
        SI_ConstStringVal("false"), SI_ConstStringVal("true"), SI_BoolVal(false),
        SI_ConstStringVal("false"), SI_NullVal(), SI_NullVal(),
        SI_ConstStringVal("true"), SI_ConstStringVal("false"), SI_BoolVal(false),
        SI_ConstStringVal("true"), SI_ConstStringVal("true"), SI_BoolVal(true),
        SI_ConstStringVal("true"), SI_NullVal(), SI_NullVal(),
        SI_NullVal(), SI_ConstStringVal("false"), SI_NullVal(),
        SI_NullVal(), SI_ConstStringVal("true"), SI_NullVal(),
        SI_NullVal(), SI_NullVal(), SI_NullVal()
    };

    for(int i = 0; i < 27; i += 3) {
        SIValue a = truth_table[i];
        SIValue b = truth_table[i + 1];
        SIValue expected = truth_table[i + 2];

        char *query;
        asprintf(&query, "RETURN %s AND %s", a.stringval, b.stringval);
        AR_ExpNode *arExp = _exp_from_query(query);
        SIValue result = AR_EXP_Evaluate(arExp, NULL);
        AR_EXP_Free(arExp);

        ASSERT_EQ(SI_TYPE(result), SI_TYPE(expected));
        if(SI_TYPE(result) != T_NULL) {
            ASSERT_EQ(result.longval, expected.longval);
        }
    }
}

TEST_F(ArithmeticTest, OR)
{
    SIValue truth_table [27] = {
        SI_ConstStringVal("false"), SI_ConstStringVal("false"), SI_BoolVal(false),
        SI_ConstStringVal("false"), SI_ConstStringVal("true"), SI_BoolVal(true),
        SI_ConstStringVal("false"), SI_NullVal(), SI_NullVal(),
        SI_ConstStringVal("true"), SI_ConstStringVal("false"), SI_BoolVal(true),
        SI_ConstStringVal("true"), SI_ConstStringVal("true"), SI_BoolVal(true),
        SI_ConstStringVal("true"), SI_NullVal(), SI_NullVal(),
        SI_NullVal(), SI_ConstStringVal("false"), SI_NullVal(),
        SI_NullVal(), SI_ConstStringVal("true"), SI_NullVal(),
        SI_NullVal(), SI_NullVal(), SI_NullVal()
    };

    for(int i = 0; i < 27; i += 3) {
        SIValue a = truth_table[i];
        SIValue b = truth_table[i + 1];
        SIValue expected = truth_table[i + 2];

        char *query;
        asprintf(&query, "RETURN %s OR %s", a.stringval, b.stringval);
        AR_ExpNode *arExp = _exp_from_query(query);
        SIValue result = AR_EXP_Evaluate(arExp, NULL);
        AR_EXP_Free(arExp);

        ASSERT_EQ(SI_TYPE(result), SI_TYPE(expected));
        if(SI_TYPE(result) != T_NULL) {
            ASSERT_EQ(result.longval, expected.longval);
        }
    }
}

TEST_F(ArithmeticTest, XOR)
{
    SIValue truth_table [27] = {
        SI_ConstStringVal("false"), SI_ConstStringVal("false"), SI_BoolVal(false),
        SI_ConstStringVal("false"), SI_ConstStringVal("true"), SI_BoolVal(true),
        SI_ConstStringVal("false"), SI_NullVal(), SI_NullVal(),
        SI_ConstStringVal("true"), SI_ConstStringVal("false"), SI_BoolVal(true),
        SI_ConstStringVal("true"), SI_ConstStringVal("true"), SI_BoolVal(false),
        SI_ConstStringVal("true"), SI_NullVal(), SI_NullVal(),
        SI_NullVal(), SI_ConstStringVal("false"), SI_NullVal(),
        SI_NullVal(), SI_ConstStringVal("true"), SI_NullVal(),
        SI_NullVal(), SI_NullVal(), SI_NullVal()
    };

    for(int i = 0; i < 27; i += 3) {
        SIValue a = truth_table[i];
        SIValue b = truth_table[i + 1];
        SIValue expected = truth_table[i + 2];

        char *query;
        asprintf(&query, "RETURN %s XOR %s", a.stringval, b.stringval);
        AR_ExpNode *arExp = _exp_from_query(query);
        SIValue result = AR_EXP_Evaluate(arExp, NULL);
        AR_EXP_Free(arExp);

        ASSERT_EQ(SI_TYPE(result), SI_TYPE(expected));
        if(SI_TYPE(result) != T_NULL) {
            ASSERT_EQ(result.longval, expected.longval);
        }
    }
}

TEST_F(ArithmeticTest, NOT)
{
SIValue truth_table [6] = {
        SI_ConstStringVal("false"), SI_BoolVal(true),
        SI_ConstStringVal("true"), SI_BoolVal(false),
        SI_NullVal(),  SI_NullVal()
    };

    for(int i = 0; i < 6; i += 2) {
        SIValue a = truth_table[i];
        SIValue expected = truth_table[i + 1];

        char *query;
        asprintf(&query, "RETURN NOT %s", a.stringval);
        AR_ExpNode *arExp = _exp_from_query(query);
        SIValue result = AR_EXP_Evaluate(arExp, NULL);
        AR_EXP_Free(arExp);

        ASSERT_EQ(SI_TYPE(result), SI_TYPE(expected));
        if(SI_TYPE(result) != T_NULL) {
            ASSERT_EQ(result.longval, expected.longval);
        }
    }
}

TEST_F(ArithmeticTest, GT)
{
    SIValue truth_table [27] = {
        SI_ConstStringVal("1"), SI_ConstStringVal("1"), SI_BoolVal(false),
        SI_ConstStringVal("1"), SI_ConstStringVal("2"), SI_BoolVal(false),
        SI_ConstStringVal("1"), SI_NullVal(), SI_NullVal(),
        SI_ConstStringVal("2"), SI_ConstStringVal("1"), SI_BoolVal(true),
        SI_ConstStringVal("2"), SI_ConstStringVal("2"), SI_BoolVal(false),
        SI_ConstStringVal("2"), SI_NullVal(), SI_NullVal(),
        SI_NullVal(), SI_ConstStringVal("1"), SI_NullVal(),
        SI_NullVal(), SI_ConstStringVal("2"), SI_NullVal(),
        SI_NullVal(), SI_NullVal(), SI_NullVal()
    };

    for(int i = 0; i < 27; i += 3) {
        SIValue a = truth_table[i];
        SIValue b = truth_table[i + 1];
        SIValue expected = truth_table[i + 2];

        char *query;
        asprintf(&query, "RETURN %s > %s", a.stringval, b.stringval);
        AR_ExpNode *arExp = _exp_from_query(query);
        SIValue result = AR_EXP_Evaluate(arExp, NULL);
        AR_EXP_Free(arExp);

        ASSERT_EQ(SI_TYPE(result), SI_TYPE(expected));
        if(SI_TYPE(result) != T_NULL) {
            ASSERT_EQ(result.longval, expected.longval);
        }
    }
}

TEST_F(ArithmeticTest, GE)
{
    SIValue truth_table [27] = {
        SI_ConstStringVal("1"), SI_ConstStringVal("1"), SI_BoolVal(true),
        SI_ConstStringVal("1"), SI_ConstStringVal("2"), SI_BoolVal(false),
        SI_ConstStringVal("1"), SI_NullVal(), SI_NullVal(),
        SI_ConstStringVal("2"), SI_ConstStringVal("1"), SI_BoolVal(true),
        SI_ConstStringVal("2"), SI_ConstStringVal("2"), SI_BoolVal(true),
        SI_ConstStringVal("2"), SI_NullVal(), SI_NullVal(),
        SI_NullVal(), SI_ConstStringVal("1"), SI_NullVal(),
        SI_NullVal(), SI_ConstStringVal("2"), SI_NullVal(),
        SI_NullVal(), SI_NullVal(), SI_NullVal()
    };

    for(int i = 0; i < 27; i += 3) {
        SIValue a = truth_table[i];
        SIValue b = truth_table[i + 1];
        SIValue expected = truth_table[i + 2];

        char *query;
        asprintf(&query, "RETURN %s >= %s", a.stringval, b.stringval);
        AR_ExpNode *arExp = _exp_from_query(query);
        SIValue result = AR_EXP_Evaluate(arExp, NULL);
        AR_EXP_Free(arExp);

        ASSERT_EQ(SI_TYPE(result), SI_TYPE(expected));
        if(SI_TYPE(result) != T_NULL) {
            ASSERT_EQ(result.longval, expected.longval);
        }
    }
}

TEST_F(ArithmeticTest, LT)
{
    SIValue truth_table [27] = {
        SI_ConstStringVal("1"), SI_ConstStringVal("1"), SI_BoolVal(false),
        SI_ConstStringVal("1"), SI_ConstStringVal("2"), SI_BoolVal(true),
        SI_ConstStringVal("1"), SI_NullVal(), SI_NullVal(),
        SI_ConstStringVal("2"), SI_ConstStringVal("1"), SI_BoolVal(false),
        SI_ConstStringVal("2"), SI_ConstStringVal("2"), SI_BoolVal(false),
        SI_ConstStringVal("2"), SI_NullVal(), SI_NullVal(),
        SI_NullVal(), SI_ConstStringVal("1"), SI_NullVal(),
        SI_NullVal(), SI_ConstStringVal("2"), SI_NullVal(),
        SI_NullVal(), SI_NullVal(), SI_NullVal()
    };

    for(int i = 0; i < 27; i += 3) {
        SIValue a = truth_table[i];
        SIValue b = truth_table[i + 1];
        SIValue expected = truth_table[i + 2];

        char *query;
        asprintf(&query, "RETURN %s < %s", a.stringval, b.stringval);
        AR_ExpNode *arExp = _exp_from_query(query);
        SIValue result = AR_EXP_Evaluate(arExp, NULL);
        AR_EXP_Free(arExp);

        ASSERT_EQ(SI_TYPE(result), SI_TYPE(expected));
        if(SI_TYPE(result) != T_NULL) {
            ASSERT_EQ(result.longval, expected.longval);
        }
    }
}

TEST_F(ArithmeticTest, LE)
{
    SIValue truth_table [27] = {
        SI_ConstStringVal("1"), SI_ConstStringVal("1"), SI_BoolVal(true),
        SI_ConstStringVal("1"), SI_ConstStringVal("2"), SI_BoolVal(true),
        SI_ConstStringVal("1"), SI_NullVal(), SI_NullVal(),
        SI_ConstStringVal("2"), SI_ConstStringVal("1"), SI_BoolVal(false),
        SI_ConstStringVal("2"), SI_ConstStringVal("2"), SI_BoolVal(true),
        SI_ConstStringVal("2"), SI_NullVal(), SI_NullVal(),
        SI_NullVal(), SI_ConstStringVal("1"), SI_NullVal(),
        SI_NullVal(), SI_ConstStringVal("2"), SI_NullVal(),
        SI_NullVal(), SI_NullVal(), SI_NullVal()
    };

    for(int i = 0; i < 27; i += 3) {
        SIValue a = truth_table[i];
        SIValue b = truth_table[i + 1];
        SIValue expected = truth_table[i + 2];

        char *query;
        asprintf(&query, "RETURN %s <= %s", a.stringval, b.stringval);
        AR_ExpNode *arExp = _exp_from_query(query);
        SIValue result = AR_EXP_Evaluate(arExp, NULL);
        AR_EXP_Free(arExp);

        ASSERT_EQ(SI_TYPE(result), SI_TYPE(expected));
        if(SI_TYPE(result) != T_NULL) {
            ASSERT_EQ(result.longval, expected.longval);
        }
    }
}

TEST_F(ArithmeticTest, EQ)
{
    SIValue truth_table [27] = {
        SI_ConstStringVal("1"), SI_ConstStringVal("1"), SI_BoolVal(true),
        SI_ConstStringVal("1"), SI_ConstStringVal("2"), SI_BoolVal(false),
        SI_ConstStringVal("1"), SI_NullVal(), SI_NullVal(),
        SI_ConstStringVal("2"), SI_ConstStringVal("1"), SI_BoolVal(false),
        SI_ConstStringVal("2"), SI_ConstStringVal("2"), SI_BoolVal(true),
        SI_ConstStringVal("2"), SI_NullVal(), SI_NullVal(),
        SI_NullVal(), SI_ConstStringVal("1"), SI_NullVal(),
        SI_NullVal(), SI_ConstStringVal("2"), SI_NullVal(),
        SI_NullVal(), SI_NullVal(), SI_NullVal()
    };

    for(int i = 0; i < 27; i += 3) {
        SIValue a = truth_table[i];
        SIValue b = truth_table[i + 1];
        SIValue expected = truth_table[i + 2];

        char *query;
        asprintf(&query, "RETURN %s = %s", a.stringval, b.stringval);
        AR_ExpNode *arExp = _exp_from_query(query);
        SIValue result = AR_EXP_Evaluate(arExp, NULL);
        AR_EXP_Free(arExp);

        ASSERT_EQ(SI_TYPE(result), SI_TYPE(expected));
        if(SI_TYPE(result) != T_NULL) {
            ASSERT_EQ(result.longval, expected.longval);
        }
    }
}

TEST_F(ArithmeticTest, NE)
{
    SIValue truth_table [27] = {
        SI_ConstStringVal("1"), SI_ConstStringVal("1"), SI_BoolVal(false),
        SI_ConstStringVal("1"), SI_ConstStringVal("2"), SI_BoolVal(true),
        SI_ConstStringVal("1"), SI_NullVal(), SI_NullVal(),
        SI_ConstStringVal("2"), SI_ConstStringVal("1"), SI_BoolVal(true),
        SI_ConstStringVal("2"), SI_ConstStringVal("2"), SI_BoolVal(false),
        SI_ConstStringVal("2"), SI_NullVal(), SI_NullVal(),
        SI_NullVal(), SI_ConstStringVal("1"), SI_NullVal(),
        SI_NullVal(), SI_ConstStringVal("2"), SI_NullVal(),
        SI_NullVal(), SI_NullVal(), SI_NullVal()
    };

    for(int i = 0; i < 27; i += 3) {
        SIValue a = truth_table[i];
        SIValue b = truth_table[i + 1];
        SIValue expected = truth_table[i + 2];

        char *query;
        asprintf(&query, "RETURN %s <> %s", a.stringval, b.stringval);
        AR_ExpNode *arExp = _exp_from_query(query);
        SIValue result = AR_EXP_Evaluate(arExp, NULL);
        AR_EXP_Free(arExp);

        ASSERT_EQ(SI_TYPE(result), SI_TYPE(expected));
        if(SI_TYPE(result) != T_NULL) {
            ASSERT_EQ(result.longval, expected.longval);
        }
    }
}
