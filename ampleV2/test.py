def myDecorator(func):
    def wrapperFunc(*args,**kargs):
        result = func(*args,**kargs)
        newResult = result+1 
        return newResult
    return wrapperFunc


@myDecorator
def ham1(x,y):
    return x+y


if __name__ =="__main__":
    result = ham1(10,y=10)
    print(result)