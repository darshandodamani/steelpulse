with open('steelpulse.py', 'rb') as f:
    content = f.read()

content = content.replace(b'\xef\xbf\xbd Tubing', b'Tubing')

with open('steelpulse.py', 'wb') as f:
    f.write(content)

print('Fixed')
