class ApiCall:

    def __init__(self,
                 url: str,
                 export_url: str,
                 collection_name: str,
                 interval: int) -> None:
        self._url = url
        self._export_url = export_url
        self._collection_name = collection_name
        self._interval = interval
        self._last_data = None
    
    def url(self) -> str:
        return self._url
    
    def export_url(self) -> str:
        return self._export_url
    
    def collection_name(self) -> str:
        return self._collection_name
    
    def interval(self) -> int:
        return self._interval
    
    def last_data(self) -> str:
        return self._last_data
    
    def set_last_data(self, last_data: str) -> None:
        self._last_data = last_data