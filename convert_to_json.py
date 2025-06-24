import yaml
import json


def convert_yaml_to_json_objects(yaml_path):
    with open(yaml_path, 'r', encoding='utf-8') as f:
        data = yaml.safe_load(f)

    result = []
    for key, value in data.items():
        q_type = value.get('type')
        label = value.get('label')
        options = value.get('options')
        # Handle matrix type
        if q_type == 'matrix':
            matrix_label = value.get('label')
            rows = value.get('row', {})
            # Normalize options to list of values
            if isinstance(options, dict):
                opt_list = [v for v in options.values()]
            else:
                opt_list = options
            for row_label, row_key in rows.items():
                obj = {
                    'key': f'rankings_{row_key}',
                    'type': 'mc',
                    'label': f"{matrix_label} {row_label}",
                    'options': opt_list
                }
                result.append(obj)
        else:
            # Normalize options to list of values if present
            if isinstance(options, dict):
                opt_list = [v for v in options.values()]
            elif isinstance(options, list):
                opt_list = options
            else:
                opt_list = None
            # If label is a list (as in rankings_og_tril), use the parent matrix label
            if isinstance(label, list) and key.startswith('rankings_'):
                # Find the parent matrix label
                parent_matrix = data.get('rankings', {})
                matrix_label = parent_matrix.get('label', '')
                # Find the row name from the key
                row_map = parent_matrix.get('row', {})
                row_name = None
                for k, v in row_map.items():
                    if f'rankings_{v}' == key:
                        row_name = k
                        break
                if row_name:
                    label = f"{matrix_label} {row_name}"
                else:
                    label = matrix_label
            obj = {
                'key': key,
                'type': q_type,
                'label': label,
            }
            if opt_list is not None:
                obj['options'] = opt_list
            result.append(obj)
    return result


# Example usage:
# objs = convert_yaml_to_json_objects('sample_q.yml')
# print(json.dumps(objs, indent=2)) 