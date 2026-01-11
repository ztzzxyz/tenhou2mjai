import os


def check_bye_event(xml_path) -> bool:
    """Check if XML file contains BYE event (player disconnection)"""
    try:
        with open(xml_path, 'r', encoding='utf-8') as f:
            content = f.read()
            return 'BYE' in content
    except Exception:
        return False

for root, dirs, files in os.walk('./xml'):
    for file in files:
        xml_path = os.path.join(root, file)
        if check_bye_event(xml_path):
            print(xml_path)
            os.remove(xml_path)
