try:
    from airflow.exceptions import AirflowFailException
except ImportError:
    # Si Airflow no está instalado, usar Exception normal
    # Esto asegura que el framework funcione standalone
    class AirflowFailException(Exception):
        pass

class QualityThresholdError(AirflowFailException):
    """Excepción específica para fallos de calidad de datos.
    Debe ser tratada como un fallo NO reintentable en orquestadores.
    """
    pass
