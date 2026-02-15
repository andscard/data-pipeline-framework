from typing import Any, Dict
import html

class ValidationFormatter:

    @staticmethod
    def _format_value(value: Any) -> str:
        if value is None:
            return "N/A"
        str_val = str(value)
        if len(str_val) > 60:
            str_val = str_val[:60] + "..."
        return html.escape(str_val)

    @staticmethod
    def get_description(exp_type: str, detail: Dict[str, Any]) -> str:
        
        kwargs = detail.get('kwargs', {})
        result = detail.get('result', detail)
        fmt = ValidationFormatter._format_value

        bad_examples = ""
        partial_unexpected = result.get('partial_unexpected_list', [])
        if partial_unexpected:
            examples_str = ", ".join([f"'{fmt(x)}'" for x in partial_unexpected[:3]])
            if len(partial_unexpected) > 3:
                examples_str += "..."
            bad_examples = f"<br><span style='color:#dc3545; font-size:0.85em'>Valores encontrados: {examples_str}</span>"

        descriptions = {
            'expect_column_values_to_match_regex': lambda k: 
                f"Formato inválido. Se encontraron valores que NO coinciden con el patrón: <code>{fmt(k.get('regex'))}</code>",
            'expect_column_values_to_not_match_regex': lambda k: 
                f"Patrón prohibido detectado: <code>{fmt(k.get('regex'))}</code>",
            'expect_column_values_to_be_of_type': lambda k: 
                f"Tipo de dato incorrecto. Se esperaba: <strong>{fmt(k.get('type_'))}</strong>",
            'expect_column_values_to_be_in_type_list': lambda k: 
                f"Tipo de dato no permitido. Tipos válidos: {fmt(k.get('type_list'))}",
            'expect_column_values_to_be_in_set': lambda k: 
                (f"Distribución insuficiente. Se esperaba que al menos el {float(k.get('mostly', 1.0))*100:.0f}% de los registros fueran: {fmt(k.get('value_set'))}" 
                 if k.get('mostly') and k.get('mostly') < 1.0 
                 else f"Valor no permitido. Debe estar en la lista: {fmt(k.get('value_set'))}" if k.get('value_set') else "Valor no permitido (conjunto de valores no definido)."),
            'expect_column_values_to_not_be_in_set': lambda k: 
                f"Valor prohibido encontrado. No debe ser ninguno de: {fmt(k.get('value_set'))}",
            'expect_column_values_to_be_between': lambda k: 
                (f"Valor fuera de rango. Debe estar entre <strong>{fmt(k.get('min_value'))}</strong> y <strong>{fmt(k.get('max_value'))}</strong>" 
                 if k.get('min_value') is not None and k.get('max_value') is not None else
                 (f"Valor muy bajo. Debe ser mayor o igual a <strong>{fmt(k.get('min_value'))}</strong>" if k.get('min_value') is not None else
                  f"Valor muy alto. Debe ser menor o igual a <strong>{fmt(k.get('max_value'))}</strong>")),
            'expect_column_values_to_not_be_null': lambda k: 
                "Valores nulos (NULL) encontrados en columna obligatoria.",
            'expect_column_values_to_be_null': lambda k: 
                "Valores no nulos encontrados en columna que debería estar vacía.",
            'expect_column_values_to_be_unique': lambda k: 
                "Valores duplicados encontrados (la columna debe ser única).",
            'expect_compound_columns_to_be_unique': lambda k: 
                f"Duplicados encontrados en la combinación de columnas: {fmt(k.get('column_list'))}",
            'expect_column_value_lengths_to_be_between': lambda k: 
                f"Longitud incorrecta. Debe tener entre {fmt(k.get('min_value'))} y {fmt(k.get('max_value'))} caracteres.",
            'expect_column_value_lengths_to_equal': lambda k: 
                f"Longitud incorrecta. Debe tener exactamente {fmt(k.get('value'))} caracteres.",
            'expect_column_mean_to_be_between': lambda k: 
                f"Promedio ({fmt(result.get('observed_value', 'N/A'))}) fuera de rango [{fmt(k.get('min_value'))}, {fmt(k.get('max_value'))}].",
             'expect_table_row_count_to_be_between': lambda k: 
                f"Volumen de datos insuficiente o excedido. Filas encontradas: {fmt(result.get('observed_value', 'N/A'))} (Esperado: {fmt(k.get('min_value'))}-{fmt(k.get('max_value'))}).",
             'expect_column_values_to_be_monotonic': lambda k:
                f"La columna no es monotónicamente {fmt(k.get('direction', 'increasing'))}.",
             'expect_column_pair_values_A_to_be_greater_than_B': lambda k:
                f"Inconsistencia cronológica o lógica. '{fmt(k.get('column_A'))}' debe ser mayor que '{fmt(k.get('column_B'))}'.",
             'expect_column_pair_values_a_to_be_greater_than_b': lambda k:
                f"Inconsistencia cronológica o lógica. '{fmt(k.get('column_A'))}' debe ser mayor que '{fmt(k.get('column_B'))}'.",
             'expect_column_values_to_be_in_reference_set': lambda k:
                f"Violación de integridad referencial. Valores no encontrados en dataset '{fmt(k.get('reference_dataset'))}'.",
        }
        
        base_desc = ""

        if 'row_condition' in kwargs:
             condition = kwargs['row_condition']
             formatter = descriptions.get(exp_type)
             if formatter:
                 try:
                     inner_desc = formatter(kwargs)
                     base_desc = f"<strong>Regla de Negocio (Si {condition}):</strong> {inner_desc}"
                 except: pass

        if not base_desc:
            formatter = descriptions.get(exp_type)
            if formatter:
                try:
                    base_desc = formatter(kwargs)
                except Exception:
                    base_desc = exp_type
            else:
                clean_name = exp_type.replace('expect_column_values_to_', '').replace('expect_column_', '').replace('expect_table_', '').replace('_', ' ').capitalize()
                base_desc = f"Fallo en validación: {clean_name}"
            
        return f"{base_desc}{bad_examples}"
