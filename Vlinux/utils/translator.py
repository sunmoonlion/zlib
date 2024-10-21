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
text_to_translate = "Unfortunately (or fortunately to better understand the platform) there is an open position on the last day of the Data Feed. Even if a SELL operation has been sent … IT HAS NOT YET BEEN EXECUTED."
translated_text = translate_text(text_to_translate, source_lang='en', target_lang='zh')
print("Translated Text:", translated_text)
