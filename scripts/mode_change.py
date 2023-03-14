import os
import sys

if len(sys.argv) != 2:
    print("Command: python3 mode_change.py [MODE]")
    exit(0)

original = open("src/execution_plan/ops/op_conditional_traverse.c", "r")
modified = open("src/execution_plan/ops/op_conditional_traverse_modified.c", "w")

# Copy
in_pattern = False
for line in original:
    # Detect pattern
    if line == "/*start_mode_configuration*/\n":
        modified.write(line)
        in_pattern = True
    elif line == "/*end_mode_configuration*/\n":
        modified.write(f"#define {sys.argv[1]}\n")
        modified.write(line)
        in_pattern = False
    elif in_pattern:
        pass
    else:
        modified.write(line)

# Remove old files
os.unlink("src/execution_plan/ops/op_conditional_traverse.c")
os.rename("src/execution_plan/ops/op_conditional_traverse_modified.c", "src/execution_plan/ops/op_conditional_traverse.c")