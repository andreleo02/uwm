class ApiCall:

    def __init__(self,
                 url: str,
                 collection_name: str,
                 interval: int,
                 save_on_mongo: bool,
                 save_on_postgres: bool) -> None:
        self._url = url
        self._collection_name = collection_name
        self._interval = interval
        self._save_on_mongo = save_on_mongo
        self._save_on_postgres = save_on_postgres
        self._last_data = None
    
    def url(self) -> str:
        return self._url
    
    def collection_name(self) -> str:
        return self._collection_name
    
    def interval(self) -> int:
        return self._interval
    
    def save_on_mongo(self) -> bool:
        return self._save_on_mongo
    
    def save_on_postgres(self) -> bool:
        return self._save_on_postgres
    
    def last_data(self) -> str:
        return self._last_data
    
    def set_last_data(self, last_data: str) -> None:
        self._last_data = last_data