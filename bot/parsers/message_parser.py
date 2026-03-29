import re
from dataclasses import dataclass
from typing import Optional

@dataclass
class ParsedTobacco:
    raw_brand: str
    raw_flavor: str
    percentage: Optional[float]
    grams: Optional[str] = None  # граммовка если указана без %

@dataclass
class ParsedMix:
    tobaccos: list[ParsedTobacco]
    original_text: str


BRAND_ALIASES = {
    "musthave": "MustHave", "must have": "MustHave", "must": "MustHave",
    "мастхев": "MustHave", "мх": "MustHave", "mh": "MustHave",
    "jent": "Jent", "джент": "Jent",
    "darkside": "Darkside", "dark side": "Darkside", "дс": "Darkside",
    "ds": "Darkside", "дарксайд": "Darkside",
    "burn": "Burn", "берн": "Burn", "burn black": "Burn Black",
    "muassel": "Muassel", "муасель": "Muassel", "муассель": "Muassel",
    "palitra": "Palitra", "палитра": "Palitra",
    "hook": "Hook", "хук": "Hook",
    "spectrum": "Spectrum", "спектрум": "Spectrum",
    "overdose": "Overdose", "овердоз": "Overdose",
    "adalya": "Adalya", "адалья": "Adalya",
    "starline": "Starline", "старлайн": "Starline",
    "endorphin": "Endorphin", "эндорфин": "Endorphin",
    "satyr": "Satyr", "сатир": "Satyr",
    "brusko": "Brusko", "бруско": "Brusko",
    "duft": "DUFT", "дуфт": "DUFT",
    "deus": "DEUS", "деус": "DEUS",
    "dogma": "Dogma", "догма": "Dogma",
    "husky": "Husky", "хаски": "Husky",
    "joy": "Joy", "джой": "Joy",
    "morpheus": "Morpheus", "морфей": "Morpheus",
    "mr brew": "Mr. Brew", "mr. brew": "Mr. Brew", "мистер брю": "Mr. Brew",
    "северный": "Северный", "severniy": "Северный",
    "хулиган": "Хулиган", "hooligan": "Хулиган",
    "энтузиаст": "Энтузиаст",
    "сарма": "Сарма", "sarma": "Сарма",
    "наш": "НАШ", "nash": "НАШ",
    "snobless": "Snobless",
    "trofimoff": "Trofimoff", "трофимов": "Trofimoff",
    "душа": "Душа",
    "codex": "Codex Nubium", "codex nubium": "Codex Nubium",
    "sapphire": "SAPPHIRE CROWN", "sapphire crown": "SAPPHIRE CROWN",
    "jam": "Jam", "джем": "Jam",
    "летим": "Летим", "letim": "Летим",
    "chaba": "Chaba", "чаба": "Chaba",
    "chabacco": "Chabacco", "чабакко": "Chabacco",
}

NOISE_WORDS = {
    'взял', 'все', 'от', 'из', 'и', 'на', 'с', 'по', 'для', 'это', 'что',
    'как', 'так', 'но', 'а', 'в', 'к', 'у', 'за', 'уголь', 'кальян',
    'чаша', 'миксочек', 'микс', 'mix', 'рецепт', 'состав', 'процентовка',
    'попробовал', 'курил', 'думаю', 'помнится', 'советую',
    'итого', 'итог', 'всего', 'получилось', 'вышло',
}

NOISE_PHRASES = [
    'процентовк', 'давайте', 'сперва', 'помнится', 'пробовал', 'получилась',
    'хуке', 'углях', 'чашечки', 'вашим', 'вечер', 'вечерний',
    'интересно', 'ахуенно', 'дополнени', 'неожиданн', 'запечённ', 'божечки',
    'кошечки', 'раскрылась', 'сыграла',
    'знаете', 'скажу', 'подкинул', 'закинуть', 'заметочку',
    'общем', 'целом', 'выходе', 'получаем', 'весьма', 'основу', 'лимонадную',
    'даёт', 'работают', 'увидел', 'решил', 'собрать', 'приехал',
    'жара', 'вкусный', 'вкусно', 'надо', 'заметочк', 'попсово', 'олдово',
    'забить', 'захотелось', 'старенькое', 'напомню', 'забыли', 'классику',
    'аромат', 'благо', 'нужных', 'внутри',
]


def normalize_brand(text: str) -> Optional[str]:
    lower = text.lower().strip()
    if lower in BRAND_ALIASES:
        return BRAND_ALIASES[lower]
    for alias in sorted(BRAND_ALIASES.keys(), key=len, reverse=True):
        if lower == alias or lower.startswith(alias + ' '):
            return BRAND_ALIASES[alias]
    return None


def find_brand_in_line(line: str) -> Optional[str]:
    """Ищет любое упоминание бренда в строке."""
    lower = line.lower()
    for alias in sorted(BRAND_ALIASES.keys(), key=len, reverse=True):
        if alias in lower:
            return BRAND_ALIASES[alias]
    return None


def extract_percentage(text: str) -> tuple[Optional[float], str]:
    """Извлекает процент (только если есть символ %), возвращает (процент, строка_без_процента)."""
    match = re.search(r'(\d+(?:\.\d+)?)\s*%', text)
    if match:
        pct = float(match.group(1))
        clean = (text[:match.start()] + text[match.end():]).strip()
        return pct, clean
    return None, text


def extract_trailing_number(text: str) -> tuple[Optional[str], str]:
    """
    Если строка заканчивается числом БЕЗ % — это граммовка.
    'Сахарный арбуз 40' -> ('40 гр', 'Сахарный арбуз')
    'Мята 20'           -> ('20 гр', 'Мята')
    'Vanilla Cream'     -> (None, 'Vanilla Cream')
    """
    match = re.search(r'\s+(\d+)\s*(?:гр|г|g|gr)?\s*$', text, re.IGNORECASE)
    if match:
        num = int(match.group(1))
        # Граммовки обычно 20-250, проценты 1-100 — но без % это граммы
        clean = text[:match.start()].strip()
        return f"{num} гр", clean
    return None, text


def extract_grams_marker(text: str) -> str:
    """Убирает граммовку с суффиксом (20гр, 25g) из строки."""
    return re.sub(r'\s*\d+\s*(?:гр|г|g|gr)\.?\s*$', '', text, flags=re.IGNORECASE).strip()


def is_noise_line(line: str) -> bool:
    """Проверяет, является ли строка мусорной."""
    line_lower = line.lower().strip()
    if not line_lower:
        return True
    # Слишком длинная
    if len(line_lower) > 50:
        return True
    # Содержит маркеры мусора
    if any(phrase in line_lower for phrase in NOISE_PHRASES):
        return True
    # Только мусорные слова
    words = set(re.findall(r'[а-яёa-z]+', line_lower))
    if words and words.issubset(NOISE_WORDS):
        return True
    # Служебные цифры (время, счётчик)
    if re.match(r'^\d+[\s:]\d+$', line_lower):
        return True
    return False


def _find_last_tobacco_line_idx(lines: list[str]) -> int:
    """
    Находит индекс последней строки которая выглядит как табак
    (содержит % ИЛИ заканчивается числом после текста).
    """
    last = -1
    for i, line in enumerate(lines):
        stripped = line.strip()
        if not stripped:
            continue
        # Строка с процентом
        if re.search(r'\d+\s*%', stripped):
            last = i
            continue
        # Строка вида "Текст число" (граммовка)
        if re.search(r'^[А-Яа-яёA-Za-z].*\s+\d+\s*(?:гр|г|g|gr)?\s*$', stripped, re.IGNORECASE):
            last = i
    return last


def parse_mix_from_text(text: str) -> Optional[ParsedMix]:
    """
    Универсальный парсер миксов. Поддерживает форматы:

    1. Бренд + вкус + %:   "Jent follar 70%"
    2. Бренд + вкус + граммы:  "Overdose Сахарный арбуз 40"
    3. Бренд в контексте:
         "Взял все от MustHave"
         "Ваниль 30%"
         "Груша"
    4. Смешанный.
    """
    lines = text.strip().split('\n')
    tobaccos = []

    # --- Шаг 1: ищем контекстный бренд (первое упоминание бренда в тексте) ---
    context_brand: Optional[str] = None
    for line in lines:
        found = find_brand_in_line(line.strip())
        if found:
            context_brand = found
            break

    # --- Шаг 2: обрезаем хвост — всё после последней "табачной" строки ---
    last_idx = _find_last_tobacco_line_idx(lines)
    if last_idx >= 0:
        lines = lines[:last_idx + 1]

    # --- Шаг 3: парсим строки ---
    current_brand = context_brand

    for line in lines:
        line = line.strip()
        if not line:
            continue

        # Пробуем извлечь % (только если есть знак %)
        percentage, line_no_pct = extract_percentage(line)

        # Если % не было — пробуем извлечь граммы (число в конце)
        grams = None
        if percentage is None:
            grams, line_no_pct = extract_trailing_number(line_no_pct)

        # Чистим оставшиеся граммовые суффиксы
        line_clean = extract_grams_marker(line_no_pct)

        if not line_clean:
            continue

        # Строка = Бренд + Вкус? (проверяем первым — важнее чем бренд-alone)
        parsed_inline = _try_parse_brand_flavor(line_clean)
        if parsed_inline:
            brand, flavor = parsed_inline
            current_brand = brand
            if not is_noise_line(flavor):
                tobaccos.append(ParsedTobacco(
                    raw_brand=brand,
                    raw_flavor=flavor,
                    percentage=percentage,
                    grams=grams
                ))
            continue

        # Вся строка — только бренд?
        brand_only = normalize_brand(line_clean)
        if brand_only:
            current_brand = brand_only
            continue

        # Строка содержит бренд внутри (контекстная, без числа)
        inline_brand = find_brand_in_line(line_clean)
        if inline_brand and percentage is None and grams is None:
            current_brand = inline_brand
            continue

        # Строка — это вкус
        if is_noise_line(line_clean):
            continue

        if current_brand:
            tobaccos.append(ParsedTobacco(
                raw_brand=current_brand,
                raw_flavor=line_clean,
                percentage=percentage,
                grams=grams
            ))

    if not tobaccos:
        return None

    return ParsedMix(tobaccos=tobaccos, original_text=text)


def _try_parse_brand_flavor(text: str) -> Optional[tuple[str, str]]:
    """Пробует разобрать строку как 'Бренд вкус'."""
    words = text.split()
    for n in range(min(3, len(words)), 0, -1):
        candidate = ' '.join(words[:n])
        brand = normalize_brand(candidate)
        if brand:
            flavor = ' '.join(words[n:])
            if flavor:
                return brand, flavor
    return None
