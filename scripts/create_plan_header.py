import os

label_map = {
    "": -1,
    "Continent": 0,
    "Country": 1,
    "City": 2,
    "University": 3,
    "Company": 4,
    "TagClass": 5,
    "Tag": 6,
    "Forum": 7,
    "Person": 8,
    "Comment": 9,
    "Post": 10,
    "Any": 99999
}

relationship_map = {
    "": -1,
    "IS_PART_OF": 0,
    "IS_LOCATED_IN": 1,
    "IS_SUBCLASS_OF": 2,
    "HAS_TYPE": 3,
    "HAS_CREATOR": 4,
    "REPLY_OF": 5,
    "CONTAINER_OF": 6,
    "HAS_MEMBER": 7,
    "HAS_MODERATOR": 8,
    "HAS_TAG": 9,
    "HAS_INTEREST": 10,
    "KNOWS": 11,
    "LIKES": 12,
    "STUDY_AT": 13,
    "WORK_AT": 14
}

direction_map = {
    "": -1,
    "Forward": 1,
    "Backward": 2,
    "Any": 3
}

direction_enum_map = {
    -1: "-1",
    1: "DIR_F",
    2: "DIR_B",
    3: "DIR_FB"
}

polarity_enum_map = {
    -1: "-1",
    0: "DIR_POS",
    1: "DIR_NEG"
}

def convert_id(id):
    if id == 0:
        return 0
    else:
        return (2 * id) - 1


def create_plan_header():
    return_str = []

    f = open("/home/ykerdcha/RedisGraph/scripts/isqb.csv", mode="r", encoding="utf-8-sig")

    query_headers = []

    current_query = 0
    current_header = {}

    current_label = -99
    current_relationships = []
    current_optional = []
    current_directions = []
    current_is_negative = []
    current_connected_nodes = []
    current_negative_nodes = []

    for line in f:
        tokens = line.strip().split(",")

        # Next query
        if tokens[0] != '':
            # Previous query is new
            if current_header != {}:
                current_header["relationships"].append(current_relationships)
                current_header["directions"].append(current_directions)
                current_header["negativities"].append(current_is_negative)
                current_header["vertices"].append(current_connected_nodes)
                current_header["labels"].append(current_label)
                current_header["filters"].append(current_negative_nodes)
                current_header["options"].append(current_optional)

                query_headers.append(current_header)
            
            # Initialize new query
            current_query = int(tokens[0])
            current_header = {
                "relationships": [],
                "directions": [],
                "negativities": [],
                "vertices": [],
                "labels": [],
                "filters": [],
                "options": []
            }

            current_label = -99
            current_relationships = []
            current_optional = []
            current_directions = []
            current_is_negative = []
            current_connected_nodes = []
            current_negative_nodes = []
        
        # New vertex
        if tokens[1] != '':
            # Append previous vertex
            if current_label != -99:
                # Append
                current_header["relationships"].append(current_relationships)
                current_header["directions"].append(current_directions)
                current_header["negativities"].append(current_is_negative)
                current_header["vertices"].append(current_connected_nodes)
                current_header["labels"].append(current_label)
                current_header["filters"].append(current_negative_nodes)
                current_header["options"].append(current_optional)

            # Reset
            current_relationships = []
            current_optional = []
            current_directions = []
            current_is_negative = []
            current_connected_nodes = []

            current_label = label_map[tokens[2]]
            current_negative_nodes = [convert_id(int(x) - 1) for x in tokens[6].split('|')] if tokens[6] != '' else []

        if tokens[3] != '':
            current_relationships.append(relationship_map[tokens[3]])
        if tokens[7] != '':
            current_directions.append(direction_map[tokens[7]])
        if tokens[5] != '':
            current_connected_nodes.append(convert_id(int(tokens[5]) - 1))
        
        current_optional.append(1 if tokens[4] == '1' else 0)
        current_is_negative.append(1 if tokens[8] == '1' else 0)
    
    current_header["relationships"].append(current_relationships)
    current_header["directions"].append(current_directions)
    current_header["negativities"].append(current_is_negative)
    current_header["vertices"].append(current_connected_nodes)
    current_header["labels"].append(current_label)
    current_header["filters"].append(current_negative_nodes)
    current_header["options"].append(current_optional)
    query_headers.append(current_header)

    # Create PARTICIPATING_RELATIONSHIPS
    return_str.append("\n")
    return_str.append("const int PARTICIPATING_RELATIONSHIPS[100][20][20] = {")
    for query_header in query_headers:
        return_str.append("{")
        for j, xs in enumerate(query_header["relationships"]):
            return_str.append("{")
            for i, x in enumerate(xs):
                return_str.append(f"{x},")
            return_str.append("-1},")
            if j > 0:
                return_str.append("{},")
        return_str.append("{}},")
    return_str.append("{}};")

    # Create PARTICIPATING_DIRECTION
    return_str.append("\n")
    return_str.append("const int PARTICIPATING_DIRECTION[100][20][20] = {")
    for query_header in query_headers:
        return_str.append("{")
        for j, xs in enumerate(query_header["directions"]):
            return_str.append("{")
            for i, x in enumerate(xs):
                return_str.append(f"{direction_enum_map[x]},")
            return_str.append("-1},")
            if j > 0:
                return_str.append("{},")
        return_str.append("{}},")
    return_str.append("{}};")

    # Create EDGE_POLARITY
    return_str.append("\n")
    return_str.append("const int EDGE_POLARITY[100][20][20] = {")
    for query_header in query_headers:
        return_str.append("{")
        for j, xs in enumerate(query_header["negativities"]):
            return_str.append("{")
            for i, x in enumerate(xs):
                return_str.append(f"{polarity_enum_map[x]},")
            return_str.append("-1},")
            if j > 0:
                return_str.append("{},")
        return_str.append("{}},")
    return_str.append("{}};")

    # Create PARTICIPATING_VERTICES
    return_str.append("\n")
    return_str.append("const int PARTICIPATING_VERTICES[100][20][20] = {")
    for query_header in query_headers:
        return_str.append("{")
        for j, xs in enumerate(query_header["vertices"]):
            return_str.append("{")
            for i, x in enumerate(xs):
                return_str.append(f"{x},")
            return_str.append("-1},")
            if j > 0:
                return_str.append("{},")
        return_str.append("{}},")
    return_str.append("{}};")

    # Create TARGET_LABEL
    return_str.append("\n")
    return_str.append("const int TARGET_LABEL[100][20] = {")
    for query_header in query_headers:
        return_str.append("{")
        for i, label in enumerate(query_header["labels"]):
            if i <= 1:
                return_str.append(f"{label},")
            else:
                return_str.append("-1,")
                return_str.append(f"{label},")
        return_str.append("-1},")
    return_str.append("{}};")

    # Create NEGATIVE_VERTICES
    return_str.append("\n")
    return_str.append("const int NEGATIVE_VERTICES[100][20][20] = {")
    for query_header in query_headers:
        return_str.append("{")
        for j, xs in enumerate(query_header["filters"]):
            return_str.append("{")
            for i, x in enumerate(xs):
                return_str.append(f"{x},")
            return_str.append("-1},")
            if j > 0:
                return_str.append("{},")
        return_str.append("{}},")
    return_str.append("{}};")

    # Create OPTIONAL_EDGES
    return_str.append("\n")
    return_str.append("const int OPTIONAL_EDGES[100][20][20] = {")
    for query_header in query_headers:
        return_str.append("{")
        for j, xs in enumerate(query_header["options"]):
            return_str.append("{")
            for i, x in enumerate(xs):
                return_str.append(f"{x},")
            return_str.append("-1},")
            if j > 0:
                return_str.append("{},")
        return_str.append("{}},")
    return_str.append("{}};")

    return return_str


# Change Conditional Traverse
original = open(f"/home/ykerdcha/RedisGraph/src/execution_plan/ops/query_plan.h", "r")
modified = open("/home/ykerdcha/RedisGraph/src/execution_plan/ops/query_plan_modified.h", "w")

generated_lines = create_plan_header()
# print(generated_lines)

# Copy
in_pattern = False
for line in original:
    # Detect pattern
    if line == "/*start_query_plan*/\n":
        modified.write(line)
        in_pattern = True
        for generated_line in generated_lines:
            modified.write(generated_line)
    elif line == "/*end_query_plan*/\n":
        modified.write("\n\n")
        modified.write(line)
        in_pattern = False
    elif in_pattern:
        pass
    else:
        modified.write(line)

# Remove old files
os.unlink("/home/ykerdcha/RedisGraph/src/execution_plan/ops/query_plan.h")
os.rename("/home/ykerdcha/RedisGraph/src/execution_plan/ops/query_plan_modified.h", "/home/ykerdcha/RedisGraph/src/execution_plan/ops/query_plan.h")
