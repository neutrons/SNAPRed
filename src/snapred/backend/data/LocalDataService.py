from snapred.meta.Singleton import Singleton

"""
    Looks up data on disk
    TBD the interface such that it is fairly generic
    but intersects that of the potential oncat data service interface
"""
@Singleton
class LocalDataService:
    def __init__(self):
        pass