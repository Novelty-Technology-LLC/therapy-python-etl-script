def get_colspecs_from_rpt(path):
    with open(path) as f:
        header_line = f.readline()
        separator_line = f.readline()

    # Remove newline characters
    header = header_line.rstrip("\n\r")
    separator = separator_line.rstrip("\n\r")

    # Find all dash segments in separator
    colspecs = []
    i = 0
    max_len = max(len(header), len(separator))

    # Pad separator if header is longer
    separator = separator.ljust(max_len)

    while i < len(separator):
        if i < len(separator) and separator[i] == "-":
            start = i
            # Find end of current dash segment
            while i < len(separator) and separator[i] == "-":
                i += 1

            # Extend to next column start or end of line
            end = i

            # Look ahead to find actual column boundary
            # (where next dashes start or end of line)
            while end < max_len and (end >= len(separator) or separator[end] == " "):
                end += 1

            colspecs.append((start, end))
        else:
            i += 1

    # # Extract column names from header using the colspecs
    # col_names = []
    # for start, end in colspecs:
    #     if start < len(header):
    #         col_name = header[start:end].strip()
    #         col_names.append(col_name if col_name else f"COL_{start}")

    return colspecs
