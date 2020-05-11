import json
text = open('config.info')
config = json.loads(text.read())
text.close()
