# (See: https://stackoverflow.com/questions/1697501/staticmethod-with-property)


class classproperty(property):
    def __get__(self, cls, owner):
        return classmethod(self.fget).__get__(None, owner)()
