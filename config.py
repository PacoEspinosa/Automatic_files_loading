import json
ambiente = input('selecciona el ambiente a conectar (prod/dev)')
if ambiente == 'prod':
    text = open('config_prod.info')
    config = json.loads(text.read())
    text.close()
elif ambiente == 'dev':
    text = open('config_dev.info')
    config = json.loads(text.read())
    text.close()
else:
    print('Debes especificar un ambiente a conectar')