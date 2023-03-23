import os
import sys

if len(sys.argv) != 3:
    print("Command: python3 mode_change.py [TRAVERSE_MODE_ID] [QUERY_PLAN_ID]")
    exit(0)

original = open(f"/home/ykerdcha/RedisGraph/src/execution_plan/ops/op_conditional_traverse.c.{sys.argv[1]}", "r")
modified = open("/home/ykerdcha/RedisGraph/src/execution_plan/ops/op_conditional_traverse_modified.c", "w")

# Copy
in_pattern = False
for line in original:
    # Detect pattern
    if line == "/*start_query_plan*/\n":
        modified.write(line)
        in_pattern = True
    elif line == "/*end_query_plan*/\n":
        modified.write(f"#define QPLAN_ID {sys.argv[2]}\n")
        modified.write(line)
        in_pattern = False
    elif in_pattern:
        pass
    else:
        modified.write(line)

# Remove old files
os.unlink("/home/ykerdcha/RedisGraph/src/execution_plan/ops/op_conditional_traverse.c")
os.rename("/home/ykerdcha/RedisGraph/src/execution_plan/ops/op_conditional_traverse_modified.c", "/home/ykerdcha/RedisGraph/src/execution_plan/ops/op_conditional_traverse.c")