# utils.py
import csv

def load_csv_dict(path):
    data = {}
    with open(path) as f:
        reader = csv.reader(f)
        header = next(reader)

        for row in reader:
            key = int(row[0])
            values = [float(x) for x in row[1:]]
            data[key] = values

    return data


def expand_config_list(config_value, available_keys):
    """
    Expand config value to list of IDs.
    
    Supports:
    - "all": all available IDs
    - [1, 2, 3]: explicit list
    - [1, "5:10"]: mixed list and ranges
    - ["all"]: all available IDs
    - "5:10": range notation (inclusive)
    - "5-10": range notation (inclusive)
    
    Args:
        config_value: Value from config (string or list)
        available_keys: Set or list of all available IDs from CSV
    
    Returns:
        Sorted list of integer IDs
    """
    if isinstance(config_value, str):
        if config_value.lower() == "all":
            return sorted(list(available_keys))
        else:
            # Single range like "5:10" or "5-10"
            return _parse_range(config_value, available_keys)
    
    elif isinstance(config_value, list):
        # Handle special case of ["all"]
        if len(config_value) == 1 and isinstance(config_value[0], str) and config_value[0].lower() == "all":
            return sorted(list(available_keys))
        
        result = []
        for item in config_value:
            if isinstance(item, int):
                if item in available_keys:
                    result.append(item)
                else:
                    raise ValueError(f"ID {item} not found in available keys: {sorted(available_keys)}")
            elif isinstance(item, str):
                if item.lower() == "all":
                    result.extend(sorted(list(available_keys)))
                else:
                    # Range within list
                    result.extend(_parse_range(item, available_keys))
        return sorted(list(set(result)))  # Remove duplicates and sort
    
    else:
        raise ValueError(f"Invalid config format: {config_value}. Expected string or list.")


def _parse_range(range_str, available_keys):
    """
    Parse range string like "5:10" or "5-10" (inclusive).
    
    Args:
        range_str: String like "5:10" or "5-10"
        available_keys: Set or list of available IDs
    
    Returns:
        List of integer IDs in range that exist in available_keys
    """
    # Support both : and - as separators
    if ":" in range_str:
        parts = range_str.split(":")
    elif "-" in range_str:
        parts = range_str.split("-")
    else:
        raise ValueError(f"Invalid range format: {range_str}. Use 'start:end' or 'start-end'")
    
    if len(parts) != 2:
        raise ValueError(f"Invalid range format: {range_str}. Use 'start:end' or 'start-end'")
    
    try:
        start = int(parts[0].strip())
        end = int(parts[1].strip())
    except ValueError:
        raise ValueError(f"Range must contain integers: {range_str}")
    
    if start > end:
        raise ValueError(f"Invalid range: start {start} > end {end}")
    
    # Return only IDs that exist in available_keys
    result = [i for i in range(start, end + 1) if i in available_keys]
    
    if not result:
        raise ValueError(f"Range {range_str} doesn't match any available IDs. Available: {sorted(available_keys)}")
    
    return result
