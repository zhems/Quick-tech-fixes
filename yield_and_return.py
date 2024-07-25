def handle_return(generator, func):
    returned = yield from generator
    func(returned)
    
def generate():
    yield 1
    yield 2
    return 3

def show_return(value):
    print('returned: {}'.format(value))

for x in handle_return(generate(), show_return):
    print(x)
