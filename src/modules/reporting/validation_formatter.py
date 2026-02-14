from typing import Any, Dict

class ValidationFormatter:
    """Helper class to format validation results with clear, human-readable descriptions."""

    @staticmethod
    def _format_value(value: Any) -> str:
        """Helper to format values for display."""
        if value is None:
            return "N/A"
        if isinstance(value, (list, tuple)):
            return str(value)[:60] + "..." if len(str(value)) > 60 else str(value)
        return str(value)

    @staticmethod
    def get_description(exp_type: str, detail: Dict[str, Any]) -> str:
        """Generates a detailed, human-readable description for an expectation failure, including example bad values."""
        
        kwargs = detail.get('kwargs', {})
        result = detail.get('result', {})
        
        # Helper for efficient formatting
        fmt = ValidationFormatter._format_value

        # --- Extracción de ejemplos de valores fallidos (Bad Data) ---
        bad_examples = ""
        partial_unexpected = result.get('partial_unexpected_list', [])
        if partial_unexpected:
            examples_str = ", ".join([f"'{str(x)}'" for x in partial_unexpected[:3]])
            if len(partial_unexpected) > 3:
                examples_str += "..."
            bad_examples = f"<br><span style='color:#dc3545; font-size:0.85em'>Valores encontrados: {examples_str}</span>"

        # --- Base Descriptions ---
        descriptions = {
            # --- Type & Format Checks ---
            'expect_column_values_to_match_regex': lambda k: 
                f"Formato inválido. Se encontraron valores que NO coinciden con el patrón: <code>{fmt(k.get('regex'))}</code>",

            'expect_column_values_to_not_match_regex': lambda k: 
                f"Patrón prohibido detectado. Valores coinciden con: <code>{fmt(k.get('regex'))}</code>",

            'expect_column_values_to_be_of_type': lambda k: 
                f"Tipo de dato incorrecto. Se esperaba: <strong>{fmt(k.get('type_'))}</strong>",

            'expect_column_values_to_be_in_type_list': lambda k: 
                f"Tipo de dato no permitido. Tipos válidos: {fmt(k.get('type_list'))}",

            # --- Set & Range Checks ---
            'expect_column_values_to_be_in_set': lambda k: 
                f"Valor no permitido. Debe estar en la lista: {fmt(k.get('value_set'))}",

            'expect_column_values_to_not_be_in_set': lambda k: 
                f"Valor prohibido encontrado. No debe ser ninguno de: {fmt(k.get('value_set'))}",

            'expect_column_values_to_be_between': lambda k: 
                f"Valor fuera de rango. Debe estar entre <strong>{fmt(k.get('min_value'))}</strong> y <strong>{fmt(k.get('max_value'))}</strong>",

            # --- Null & Uniqueness Checks ---
            'expect_column_values_to_not_be_null': lambda k: 
                "Valores nulos (NULL) encontrados en columna obligatoria.",

            'expect_column_values_to_be_null': lambda k: 
                "Valores no nulos encontrados en columna que debería estar vacía.",

            'expect_column_values_to_be_unique': lambda k: 
                "Valores duplicados encontrados (la columna debe ser única).",

            'expect_compound_columns_to_be_unique': lambda k: 
                f"Duplicados encontrados en la combinación de columnas: {fmt(k.get('column_list'))}",

            # --- String constraints ---
            'expect_column_value_lengths_to_be_between': lambda k: 
                f"Longitud incorrecta. Debe tener entre {fmt(k.get('min_value'))} y {fmt(k.get('max_value'))} caracteres.",

            'expect_column_value_lengths_to_equal': lambda k: 
                f"Longitud incorrecta. Debe tener exactamente {fmt(k.get('value'))} caracteres.",
            
            # --- Aggregate/Statistical Checks ---
            'expect_column_mean_to_be_between': lambda k: 
                f"Promedio ({fmt(result.get('observed_value', 'N/A'))}) fuera de rango [{fmt(k.get('min_value'))}, {fmt(k.get('max_value'))}].",
            
             'expect_table_row_count_to_be_between': lambda k: 
                f"Recuento de filas ({fmt(result.get('observed_value', 'N/A'))}) fuera de rango [{fmt(k.get('min_value'))}, {fmt(k.get('max_value'))}].",
        }
        
        base_desc = ""
        formatter = descriptions.get(exp_type)
        if formatter:
            try:
                base_desc = formatter(kwargs)
            except Exception:
                base_desc = exp_type
        else:
             # Fallback
            clean_name = exp_type.replace('expect_column_values_to_', '').replace('expect_column_', '').replace('expect_table_', '').replace('_', ' ').capitalize()
            base_desc = f"Fallo en validación: {clean_name}"
            
        return f"{base_desc}{bad_examples}"
