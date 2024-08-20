import requests

def translate_text(text, source_lang='auto', target_lang='en'):
    url = 'https://api.mymemory.translated.net/get'
    params = {
        'q': text,
        'langpair': f'{source_lang}|{target_lang}'  # 语言对，如 'zh|en'（中文到英文）
    }
    
    response = requests.get(url, params=params)
    response.raise_for_status()  # 检查请求是否成功
    
    data = response.json()
    return data['responseData']['translatedText']

# 示例翻译
text_to_translate = "你好，世界"
translated_text = translate_text(text_to_translate, source_lang='zh', target_lang='en')
print("Translated Text:", translated_text)
