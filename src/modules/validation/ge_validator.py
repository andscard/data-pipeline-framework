"""
Módulo de validación de calidad con Great Expectations

Este módulo proporciona funcionalidades para validar calidad de datos
utilizando Great Expectations, con soporte para generación desde configuración YAML.
"""

import pandas as pd
import great_expectations as gx
from great_expectations.core import ExpectationSuite, ExpectationConfiguration
from great_expectations.data_context import EphemeralDataContext
from typing import Dict, Any, List, Optional
import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)


class GreatExpectationsValidator:
    """Validador de calidad de datos usando Great Expectations"""
    
    def __init__(self, context_root_dir: Optional[Path] = None):
        """
        Args:
            context_root_dir: Directorio raíz para el contexto de GE.
                             Si es None, usa contexto efímero (en memoria).
        """
        if context_root_dir:
            self.context = gx.get_context(context_root_dir=str(context_root_dir))
        else:
            # Contexto efímero (en memoria, sin persistencia)
            # Great Expectations 0.18.2 requiere project_config
            project_config = {
                "config_version": 3.0,
                "plugins_directory": None,
                "evaluation_parameter_store_name": "evaluation_parameter_store",
                "expectations_store_name": "expectations_store",
                "datasources": {
                    "pandas_datasource": {
                        "class_name": "Datasource",
                        "execution_engine": {
                            "class_name": "PandasExecutionEngine"
                        },
                        "data_connectors": {
                            "default_runtime_data_connector_name": {
                                "class_name": "RuntimeDataConnector",
                                "batch_identifiers": ["default_identifier_name"]
                            }
                        }
                    }
                },
                "stores": {
                    "expectations_store": {
                        "class_name": "ExpectationsStore",
                    },
                    "validations_store": {
                        "class_name": "ValidationsStore",
                    },
                    "evaluation_parameter_store": {
                        "class_name": "EvaluationParameterStore",
                    },
                },
                "validations_store_name": "validations_store",
                "data_docs_sites": {},
            }
            self.context = EphemeralDataContext(project_config=project_config)
        
        self.validation_results = []
        self.suites = {}
    
    def build_expectation_suite_from_config(
        self, 
        suite_name: str, 
        expectations_config: List[Dict[str, Any]]
    ) -> ExpectationSuite:
        """
        Construir un Expectation Suite desde configuración YAML.
        
        Args:
            suite_name: Nombre del suite
            expectations_config: Lista de configuraciones de expectativas
        
        Returns:
            ExpectationSuite configurado
        """
        # Crear o recuperar suite
        try:
            suite = self.context.get_expectation_suite(suite_name)
            logger.info(f"Suite existente recuperado: {suite_name}")
        except Exception:
            suite = self.context.add_expectation_suite(expectation_suite_name=suite_name)
            logger.info(f"Nuevo suite creado: {suite_name}")
        
        # Agregar expectativas
        expectation_count = 0
        for exp_config in expectations_config:
            expectation = self._build_expectation(exp_config)
            if expectation:
                try:
                    suite.add_expectation(expectation)
                    expectation_count += 1
                except Exception as e:
                    logger.warning(f"No se pudo agregar expectativa: {e}")
        
        # IMPORTANTE: Actualizar el suite en el context
        self.context.add_or_update_expectation_suite(expectation_suite=suite)
        
        self.suites[suite_name] = suite
        logger.info(f"Suite '{suite_name}' configurado con {expectation_count} expectativas")
        
        return suite
    
    def _build_expectation(self, config: Dict[str, Any]) -> Optional[ExpectationConfiguration]:
        """
        Construir una expectativa desde configuración.
        
        Args:
            config: Configuración de la expectativa
        
        Returns:
            ExpectationConfiguration o None
        """
        expectation_type = config.get('expectation_type')
        if not expectation_type:
            logger.warning(f"Expectativa sin tipo: {config}")
            return None
        
        # Extraer kwargs (todos los parámetros excepto expectation_type)
        kwargs = {k: v for k, v in config.items() if k != 'expectation_type'}
        
        try:
            return ExpectationConfiguration(
                expectation_type=expectation_type,
                kwargs=kwargs
            )
        except Exception as e:
            logger.error(f"Error al crear expectativa {expectation_type}: {e}")
            return None
    
    def validate(
        self, 
        df: pd.DataFrame, 
        suite_name: str,
        dataset_name: str = "default"
    ) -> Dict[str, Any]:
        """
        Validar DataFrame contra un suite de expectativas.
        
        Args:
            df: DataFrame a validar
            suite_name: Nombre del suite de expectativas
            dataset_name: Nombre del dataset para tracking
        
        Returns:
            Diccionario con resultados de validación
        """
        try:
            # Recuperar suite
            if suite_name not in self.suites:
                logger.error(f"Suite '{suite_name}' no encontrado")
                return self._error_result(suite_name, dataset_name, "Suite no encontrado")
            
            suite = self.suites[suite_name]
            
            # API GE 0.18.x: Crear validator directamente con DataFrame
            import great_expectations as gx
            from great_expectations.core.batch import RuntimeBatchRequest
            
            # Crear batch request en runtime
            runtime_batch_request = RuntimeBatchRequest(
                datasource_name="pandas_datasource",
                data_connector_name="default_runtime_data_connector_name",
                data_asset_name=dataset_name,
                runtime_parameters={"batch_data": df},
                batch_identifiers={"default_identifier_name": dataset_name}
            )
            
            # Crear validador con el batch y el suite
            validator = self.context.get_validator(
                batch_request=runtime_batch_request,
                expectation_suite_name=suite_name
            )
            
            # Ejecutar validación
            validation_result = validator.validate()
            
            # Procesar resultados
            result = self._process_validation_result(
                validation_result, 
                suite_name, 
                dataset_name,
                len(df)
            )
            
            self.validation_results.append(result)
            
            if result['success']:
                logger.info(f"✓ Validación exitosa: {suite_name} en {dataset_name}")
            else:
                logger.warning(
                    f"✗ Validación fallida: {suite_name} en {dataset_name} "
                    f"({result['failed_expectations']}/{result['total_expectations']} fallidas)"
                )
            
            return result
            
        except Exception as e:
            logger.exception(f"Error al validar con suite '{suite_name}'")
            return self._error_result(suite_name, dataset_name, str(e))
    
    def _process_validation_result(
        self, 
        validation_result: Any, 
        suite_name: str,
        dataset_name: str,
        total_rows: int
    ) -> Dict[str, Any]:
        """
        Procesar resultado de validación de GE.
        
        Args:
            validation_result: Resultado de GE
            suite_name: Nombre del suite
            dataset_name: Nombre del dataset
            total_rows: Total de filas validadas
        
        Returns:
            Diccionario con resultados procesados
        """
        results = validation_result.results
        
        total_expectations = len(results)
        failed_expectations = sum(1 for r in results if not r.success)
        passed_expectations = total_expectations - failed_expectations
        
        # Extraer expectativas fallidas con detalles
        failed_details = []
        for result in results:
            if not result.success:
                result_dict = result.result if hasattr(result.result, '__dict__') else result.result
                
                # SOLO AGREGAR SI TIENE INFORMACIÓN ÚTIL
                # Si result_dict está vacío {}, es un falso positivo de GE
                if result_dict and len(result_dict) > 0:
                    failed_details.append({
                        'expectation_type': result.expectation_config.expectation_type,
                        'kwargs': result.expectation_config.kwargs,
                        'observed_value': result_dict.get('observed_value'),
                        'element_count': result_dict.get('element_count'),
                        'missing_count': result_dict.get('missing_count'),
                        'unexpected_count': result_dict.get('unexpected_count'),
                        'unexpected_percent': result_dict.get('unexpected_percent'),
                        'unexpected_index_list': result_dict.get('unexpected_index_list', [])[:10] if result_dict.get('unexpected_index_list') else [],
                        'partial_unexpected_list': result_dict.get('partial_unexpected_list', [])[:5] if result_dict.get('partial_unexpected_list') else []
                    })
        
        success_rate = (passed_expectations / total_expectations * 100) if total_expectations > 0 else 0
        
        return {
            'suite_name': suite_name,
            'dataset_name': dataset_name,
            'timestamp': datetime.now().isoformat(),
            'success': validation_result.success,
            'total_rows': total_rows,
            'total_expectations': total_expectations,
            'passed_expectations': passed_expectations,
            'failed_expectations': failed_expectations,
            'success_rate': success_rate,
            'failed_details': failed_details,
            'statistics': validation_result.statistics
        }
    
    def _error_result(self, suite_name: str, dataset_name: str, error_msg: str) -> Dict[str, Any]:
        """Generar resultado de error"""
        return {
            'suite_name': suite_name,
            'dataset_name': dataset_name,
            'timestamp': datetime.now().isoformat(),
            'success': False,
            'total_rows': 0,
            'total_expectations': 0,
            'passed_expectations': 0,
            'failed_expectations': 0,
            'success_rate': 0,
            'failed_details': [],
            'error': error_msg
        }
    
    def generate_data_docs(self, output_dir: Path) -> Path:
        """
        Generar Data Docs (sitio HTML interactivo).
        
        Args:
            output_dir: Directorio de salida para Data Docs
        
        Returns:
            Path al archivo index.html generado
        """
        try:
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # Configurar data docs site
            site_config = {
                "class_name": "SiteBuilder",
                "site_index_builder": {
                    "class_name": "DefaultSiteIndexBuilder",
                },
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory": str(output_dir)
                }
            }
            
            # Construir data docs
            self.context.build_data_docs()
            
            index_path = output_dir / "index.html"
            
            logger.info(f"✓ Data Docs generados en: {output_dir}")
            return index_path
            
        except Exception as e:
            logger.error(f"Error al generar Data Docs: {e}")
            raise
    
    def get_summary(self) -> Dict[str, Any]:
        """
        Obtener resumen de todas las validaciones realizadas.
        
        Returns:
            Diccionario con resumen
        """
        if not self.validation_results:
            return {
                'total_validations': 0,
                'successful_validations': 0,
                'failed_validations': 0,
                'overall_success_rate': 0
            }
        
        successful = sum(1 for r in self.validation_results if r['success'])
        failed = len(self.validation_results) - successful
        
        total_expectations = sum(r['total_expectations'] for r in self.validation_results)
        passed_expectations = sum(r['passed_expectations'] for r in self.validation_results)
        
        overall_success_rate = (
            (passed_expectations / total_expectations * 100) 
            if total_expectations > 0 else 0
        )
        
        return {
            'total_validations': len(self.validation_results),
            'successful_validations': successful,
            'failed_validations': failed,
            'total_expectations': total_expectations,
            'passed_expectations': passed_expectations,
            'failed_expectations': total_expectations - passed_expectations,
            'overall_success_rate': overall_success_rate,
            'validation_results': self.validation_results
        }


# ============================================
# HELPER: Builder desde configuración YAML
# ============================================

def build_ge_validator_from_yaml(
    quality_config: Dict[str, Any],
    context_root_dir: Optional[Path] = None
) -> GreatExpectationsValidator:
    """
    Construir validador GE desde configuración YAML.
    
    Args:
        quality_config: Sección 'quality.expectations' del YAML
        context_root_dir: Directorio raíz para contexto GE
    
    Returns:
        GreatExpectationsValidator configurado
    """
    validator = GreatExpectationsValidator(context_root_dir)
    
    expectations_configs = quality_config.get('expectations', [])
    
    for exp_config in expectations_configs:
        suite_name = exp_config.get('suite_name', 'default_suite')
        expectations = exp_config.get('expectations', [])
        
        validator.build_expectation_suite_from_config(suite_name, expectations)
    
    return validator
