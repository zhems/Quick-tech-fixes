def handle_return(generator, func):
    returned = yield from generator
    func(returned)
    
def generate():
    yield '   string 12   '
    yield 'striong 2   '
    return (3, 'string 3  ')

def show_return(value):
    print('returned: {}'.format(value))

for x in handle_return(generate(), show_return):
    print(x.strip())

# string 12
# striong 2
# returned: (3, 'string 3  ')
