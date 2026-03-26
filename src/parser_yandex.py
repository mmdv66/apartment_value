from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import re
import pickle
import os
from fake_useragent import UserAgent



class YandexRealtyParser:
    def __init__(self, headless=False, profile_dir=None):
        """
        headless: False — браузер будет видимым для ручного прохождения капчи
        profile_dir: путь к папке профиля Chrome для сохранения сессии
        """
        self.options = Options()
        
        if headless:
            self.options.add_argument('--headless=new')
        
        self.options.add_argument('--no-sandbox')
        self.options.add_argument('--disable-dev-shm-usage')
        self.options.add_argument('--disable-blink-features=AutomationControlled')
        self.options.add_argument('--disable-gpu')
        self.options.add_argument('--window-size=1920,1080')
        self.options.add_experimental_option('excludeSwitches', ['enable-automation'])
        self.options.add_experimental_option('useAutomationExtension', False)
        ua = UserAgent()
        self.options.add_argument(f'user-agent={ua.chrome}')
        self.options.add_argument('--lang=ru-RU,ru;q=0.9')
        
        if profile_dir:
            self.options.add_argument(f'--user-data-dir={profile_dir}')
            print(f"📁 Используем профиль: {profile_dir}")
        
        self.driver = None
        self.data = []
        self.profile_dir = profile_dir or './chrome_profile_yandex'
    
    def start(self):
        """Запуск браузера"""
        self.driver = webdriver.Chrome(service=Service(), options=self.options)
        
        # Скрипт для удаления признаков автоматизации
        self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
            'source': '''
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
                Object.defineProperty(navigator, 'languages', {get: () => ['ru-RU', 'ru', 'en']});
                window.chrome = {runtime: {}};
            '''
        })
        print("Chrome запущен")
    
    def close(self):
        """Закрытие браузера"""
        if self.driver:
            # Сохраняем cookies перед закрытием
            if self.profile_dir:
                cookies = self.driver.get_cookies()
                with open(os.path.join(self.profile_dir, 'cookies.pkl'), 'wb') as f:
                    pickle.dump(cookies, f)
                print(f"Cookies сохранены в {self.profile_dir}")
            
            self.driver.quit()
            print("Chrome закрыт")
    
    def wait_for_captcha_solve(self, url, timeout=300):
        """
        Ждём, пока пользователь вручную пройдёт капчу.
        Проверяем появление карточек объявлений как признак успеха.
        """
        
        self.driver.get(url)
        
        start_time = time.time()
        while time.time() - start_time < timeout:
            # Проверяем, появилась ли хотя бы одна карточка объявления
            try:
                card = self.driver.find_element(By.CSS_SELECTOR, "[data-test='OffersSerpItem']")
                if card.is_displayed():
                    print("✅ Капча пройдена! Обнаружены объявления.")
                    time.sleep(2)  # Даём странице полностью загрузиться
                    return True
            except:
                pass
            
            # Проверяем, не осталась ли капча
            try:
                captcha = self.driver.find_element(By.CSS_SELECTOR, ".CheckboxCaptcha, .smart-captcha")
                if captcha.is_displayed():
                    print("⏳ Ожидание прохождения капчи...", end='\r')
            except:
                # Капчи нет и карточки есть — успех
                pass
            
            time.sleep(1)
        
        print("\n❌ Таймаут ожидания капчи")
        return False
    
    def get_rendered_html(self):
        """Получение рендеренного HTML после прокрутки"""
        # Прокручиваем для подгрузки ленивых элементов
        for _ in range(3):
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(random.uniform(1, 2))
        
        return self.driver.page_source
    
    def clean_text(self, text):
        """Очистка текста"""
        if not text:
            return 'N/A'
        return ' '.join(text.split()).replace('\xa0', ' ').strip()
    
    def parse_price_to_int(self, price_text):
        """Конвертация цены в число"""
        if not price_text or price_text == 'N/A':
            return None
        try:
            match = re.search(r'([\d\s]+)\s*₽', str(price_text))
            if match:
                return int(re.sub(r'[^\d]', '', match.group(1)))
        except:
            pass
        return None
    
    def extract_images(self, item_soup):
        """Извлечение изображений"""
        images = {'main_image': 'N/A', 'image_urls': [], 'photo_count': 0}
        
        gallery = item_soup.find('div', {'data-test': 'SnippetGallery'})
        if not gallery:
            return images
        
        # Главное изображение
        main_img = gallery.find('img', class_=lambda x: x and 'Gallery__activeImg' in x)
        if main_img and main_img.get('src'):
            src = main_img.get('src')
            images['main_image'] = f"https:{src}" if src.startswith('//') else src
        
        # Все изображения
        img_tags = gallery.find_all('img', src=lambda x: x and 'realty-offers' in x)
        seen = set()
        for img in img_tags:
            src = img.get('src')
            if src and src not in seen:
                seen.add(src)
                full_url = f"https:{src}" if src.startswith('//') else src
                images['image_urls'].append(full_url)
        
        # Подсчёт фото
        over_limit = gallery.find('li', class_=lambda x: x and 'BulletIndicator__overLimit' in x)
        if over_limit:
            match = re.search(r'\+\s*(\d+)', over_limit.get_text())
            base_count = len(gallery.find_all('li', class_='BulletIndicator__bullet'))
            extra = int(match.group(1)) if match else 0
            images['photo_count'] = base_count + extra
        else:
            images['photo_count'] = len(images['image_urls'])
        
        return images
    
    def parse_offer(self, item_html):
        """Парсинг объявления из текстового HTML"""
        soup = BeautifulSoup(item_html, 'html.parser')
        offer = {}
        
        # Ссылка и ID — ищем по паттерну /offer/XXXXX/
        link = soup.find('a', href=lambda x: x and '/offer/' in x and re.search(r'/offer/\d+/', x))
        if link:
            href = link.get('href', '').strip()
            offer['url'] = f"https://realty.yandex.ru{href}" if href.startswith('/') else href
            # Извлекаем цифровой ID
            id_match = re.search(r'/offer/(\d+)/', href)
            offer['offer_id'] = id_match.group(1) if id_match else 'N/A'
        else:
            offer['url'] = offer['offer_id'] = 'N/A'
        
        # Получаем весь текстовый контент карточки
        text = soup.get_text(separator=' ', strip=True)
        
        # Заголовок / площадь / комнаты / этаж — из одной строки типа:
        # "40м² · 2-комнатная квартира · 2этажиз2"
        title_match = re.search(r'([\d,]+\.?\d*)\s*м²\s*·\s*([^\n·]+?)\s*·\s*(\d+)\s*этаж\s*из\s*(\d+)', text)
        if title_match:
            offer['area'] = title_match.group(1).replace(',', '.')
            offer['title'] = title_match.group(2).strip()
            offer['floor'] = f"{title_match.group(3)}/{title_match.group(4)}"
            # Определяем комнаты
            rooms_text = title_match.group(2).lower()
            if 'студия' in rooms_text or 'апартаменты-студия' in rooms_text:
                offer['rooms'] = 'студия'
            else:
                rooms_num = re.search(r'(\d+)-комн', rooms_text)
                offer['rooms'] = rooms_num.group(1) if rooms_num else 'N/A'
        else:
            offer.update({'title': 'N/A', 'area': 'N/A', 'rooms': 'N/A', 'floor': 'N/A'})
        
        # Цена — ищем паттерн "1234567₽" или "1234567₽–12%987654₽"
        price_match = re.search(r'(\d[\d\s]*\d|\d)\s*₽(?:\s*[–\-]\s*\d+%\s*)?(\d[\d\s]*\d|\d)?\s*₽?', text)
        if price_match:
            offer['price'] = price_match.group(1).strip() + ' ₽'
            offer['price_numeric'] = self.parse_price_to_int(offer['price'])
            # Старая цена (если есть скидка)
            if price_match.group(2):
                offer['old_price'] = price_match.group(2).strip() + ' ₽'
            else:
                offer['old_price'] = 'N/A'
        else:
            offer.update({'price': 'N/A', 'price_numeric': None, 'old_price': 'N/A'})
        
        # Цена за м² — ищем "ХХХ ХХХ ₽ за м²"
        ppsm_match = re.search(r'([\d\s]+)\s*₽\s*за\s*м²', text)
        offer['price_per_m2'] = ppsm_match.group(0).strip() if ppsm_match else 'N/A'
        
        # Метро — ищем ссылку с /metro-XXXX/
        metro_link = soup.find('a', href=lambda x: x and '/metro-' in x)
        offer['metro'] = self.clean_text(metro_link.get_text()) if metro_link else 'N/A'
        
        #  Время до метро — цифра + "мин" рядом с метро
        metro_time_match = re.search(r'(\d+)\s*мин', text)
        offer['metro_time'] = f"{metro_time_match.group(1)} мин" if metro_time_match else 'N/A'
        
        # Адрес — текст после этажа и до цены, исключая известные паттерны
        # Ищем по ключевым словам: улица, дом, поселок, ЖК
        address_match = re.search(r'(?:посёлок|п\.|деревня|д\.|село|с\.|ЖК|жк|[А-Я][а-я]+\s+(улица|ул\.|проспект|пр-т|переулок|пер\.|шоссе|ш\.))[^₽]+', text)
        if address_match:
            addr = address_match.group(0).strip()
            # Обрезаем лишнее
            addr = re.split(r'\s*[\d\s]+₽|Показать телефон|Написать|часов назад|минут назад', addr)[0].strip()
            offer['address'] = self.clean_text(addr)
        else:
            offer['address'] = 'N/A'
        
        # Описание — ИСПРАВЛЕННАЯ ВЕРСИЯ
    # Описание — ИСПРАВЛЕННАЯ ВЕРСИЯ
    # Сначала пробуем найти элемент по классу
        desc_el = soup.find(class_=lambda x: x and 'OffersSerpItem__description' in x)
        if desc_el and desc_el.get_text(strip=True):
            offer['description'] = self.clean_text(desc_el.get_text())
        else:
            # Fallback: ищем через data-test атрибут
            desc_el = soup.find(attrs={'data-test': lambda x: x and 'description' in x.lower()})
            if desc_el and desc_el.get_text(strip=True):
                offer['description'] = self.clean_text(desc_el.get_text())
            else:
                # Последний fallback: ищем в тексте после адреса
                # Исправленные паттерны: {10,200}? вместо {10,200}??
                desc_patterns = [
                    r'адрес[^₽]{10,200}?(?=Показать телефон|Написать|часов назад|минут назад|дней назад|[\d\s]+₽)',
                    r'квартира[^₽]{10,300}?(?=Показать телефон|Написать|часов назад|минут назад|дней назад|[\d\s]+₽)',
                ]
                for pattern in desc_patterns:
                    try:
                        match = re.search(pattern, text, re.I | re.DOTALL)
                        if match:
                            desc = match.group(0).strip()
                            if len(desc) > 30 and 'м²' not in desc[:20]:
                                offer['description'] = self.clean_text(desc[:300])
                                break
                    except re.error:
                        continue  # Пропускаем некорректный паттерн
                else:
                    offer['description'] = 'N/A'
        
        # Автор — ищем "Агентство", "Собственник" или имя
        author_match = re.search(r'(Агентство|Собственник|Риелтор|[А-Я][а-я]+\s+[А-Я][а-я]+)', text)
        offer['author'] = author_match.group(0) if author_match else 'N/A'
        
        # Дата публикации
        date_match = re.search(r'(\d+)\s*(часов?|минут?|дней?)\s*назад', text)
        if date_match:
            offer['publish_date'] = f"{date_match.group(1)} {date_match.group(2)} назад"
        else:
            # Ищем дату в формате "5 марта 2026"
            date_match2 = re.search(r'(\d{1,2}\s+[а-я]+\s+\d{4})', text, re.I)
            offer['publish_date'] = date_match2.group(0) if date_match2 else 'N/A'
        
        # Изображения — ищем ссылки на avatars.mds.yandex.net
        img_pattern = r'(https?://avatars\.mds\.yandex\.net/get-realty-offers/[^"\s\)]+)'
        images = re.findall(img_pattern, text)
        if images:
            offer['main_image'] = images[0]
            offer['image_urls'] = '; '.join(images[:5])
            offer['photo_count'] = len(images)
        else:
            # Пробуем найти через теги img
            img_tag = soup.find('img', src=lambda x: x and 'realty-offers' in x)
            if img_tag:
                src = img_tag.get('src', '')
                offer['main_image'] = f"https:{src}" if src.startswith('//') else src
                offer['image_urls'] = offer['main_image']
                offer['photo_count'] = 1
            else:
                offer.update({'main_image': 'N/A', 'image_urls': '', 'photo_count': 0})
        
        # Бейджи — "новостройка", "онлайн показ", "хорошая цена"
        badges = []
        for badge in ['новостройка', 'онлайн показ', 'хорошая цена', 'есть видео', 'торг']:
            if badge in text.lower():
                badges.append(badge)
        offer['badges'] = '; '.join(badges) if badges else 'N/A'
        
        return offer
    
    def parse_page(self, html):
        """Парсинг страницы"""
        soup = BeautifulSoup(html, 'html.parser')
        items = []
        
        # Было:
        # offers = soup.find_all('li', {'data-test': 'OffersSerpItem'})

        # Или через BeautifulSoup с дополнительной фильтрацией

        all_items = soup.find_all('li', {'data-test': 'OffersSerpItem'})
        offers = [
            item for item in all_items 
            if not item.get('hidden') 
            and 'Skeleton' not in ' '.join(item.get('class', []))
            and item.find('a', href=lambda x: x and '/offer/' in x)  # Есть реальная ссылка
        ]
        print(f"   Найдено карточек: {len(offers)}")
        
        for offer_el in offers:
            # Пропускаем рекламу
            if offer_el.get('class') and any('ad' in c.lower() for c in offer_el.get('class', [])):
                continue
            
            parsed = self.parse_offer(str(offer_el))
            if parsed and parsed.get('url') != 'N/A':
                items.append(parsed)
        
        return items
    
    def parse_multiple_pages(self, base_url, pages=3):
        print(f"Начинаем парсинг {pages} страниц")
        seen_ids = set() 
        
        for page in range(1, pages + 1):
            clean_url = re.sub(r'[?&](p|page)=\d+', '', base_url).rstrip('?&')
            url = f"{clean_url}?page={page}"
                
            print(f" Страница {page}: {url}")
            self.driver.get(url)
            
            current_url = self.driver.current_url
            print(f"  Текущий URL: {current_url}")
                
            try:
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "[data-test='OffersSerpItem']"))
                )
            except TimeoutException:
                print(f"   ⚠️ Таймаут загрузки страницы {page}")
                continue
            
            html = self.get_rendered_html()

            if html:
                items = self.parse_page(html)
                
                # Фильтрация дубликатов
                new_count = 0
                for item in items:
                    offer_id = item.get('offer_id')
                    # Пропускаем только если ID валидный и уже был
                    if offer_id != 'N/A' and offer_id in seen_ids:
                        continue
                    if offer_id != 'N/A':
                        seen_ids.add(offer_id)
                    self.data.append(item)
                    new_count += 1
                    
                print(f" Добавлено: {new_count} | Всего: {len(self.data)}")
            
            if page < pages:
                time.sleep(random.uniform(2, 4))
        
        return self.data
        
    def save_to_csv(self, filename='yandex_realty.csv'):
        """Сохранение в CSV"""
        if not self.data:
            print("\n Нет данных для сохранения")
            return None
        
        df = pd.DataFrame(self.data)
        
        # Порядок колонок
        cols = [
            'offer_id', 'price', 'price_numeric', 'old_price', 'area', 'rooms', 
            'floor', 'price_per_m2', 'metro', 'metro_time', 'address', 'author',
            'main_image', 'photo_count', 'badges', 'publish_date', 'url'
        ]
        existing = [c for c in cols if c in df.columns]
        other = [c for c in df.columns if c not in cols]
        df = df[existing + other]
        
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"\n Сохранено {len(self.data)} записей в {filename}")
        return df


    def parse_page_with_selenium(self):
        """Парсинг только видимых объявлений через Selenium"""
        items = []
        
        # Ждём загрузки
        WebDriverWait(self.driver, 10).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "[data-test='OffersSerpItem']"))
        )
        
        # Получаем только видимые элементы
        visible_cards = self.driver.find_elements(
            By.CSS_SELECTOR, 
            "li[data-test='OffersSerpItem']"
        )
        
        for card in visible_cards:
            if card.is_displayed():
                html = card.get_attribute('outerHTML')
                parsed = self.parse_offer(html)
                if parsed and parsed.get('offer_id') != 'N/A':
                    items.append(parsed)
        
        return items

if __name__ == '__main__':
    URL = "https://realty.yandex.ru/moskva_i_moskovskaya_oblast/kupit/kvartira/"
    
    PROFILE_DIR = './chrome_profile_yandex'
    os.makedirs(PROFILE_DIR, exist_ok=True)        
    parser = YandexRealtyParser(headless=False, profile_dir=PROFILE_DIR)
    parser.start()
        
    try:
        items = parser.parse_multiple_pages(URL, pages=10)
        parser.save_to_csv('yandex_realty_manual.csv')
            
    finally:
        parser.close()            