# from datasets import load_dataset

# # load only FOOD split in streaming mode
# dataset = load_dataset(
#     "openfoodfacts/product-database",
#     split="food",
#     streaming=True
# )

# # get the FIRST product record
# first_product = next(iter(dataset))

# # print all attribute names (keys)
# print("Attributes (fields) available in Open Food Facts product:\n")
# for key in first_product.keys():
#     print(key)


# from datasets import load_dataset
# import re

# dataset = load_dataset(
#     "openfoodfacts/product-database",
#     split="food",
#     streaming=True
# )

# def clean_html(text):
#     return re.sub(r"<.*?>", "", text)

# def extract_text_by_lang(data, preferred_lang="en"):
#     """
#     Extract text from list of {lang, text}
#     """
#     if isinstance(data, list):
#         # prefer English
#         for item in data:
#             if isinstance(item, dict) and item.get("lang") == preferred_lang:
#                 return item.get("text", "")
#         # fallback to first available
#         for item in data:
#             if isinstance(item, dict) and "text" in item:
#                 return item["text"]

#     if isinstance(data, str):
#         return data

#     return ""

# def extract_ingredients(raw_ingredients):
#     text = extract_text_by_lang(raw_ingredients)
#     text = clean_html(text)

#     if not text:
#         return []

#     return [i.strip() for i in text.split(",") if i.strip()]

# def fetch_product_by_name(search_name):
#     search_name = search_name.lower()

#     for product in dataset:
#         product_name = extract_text_by_lang(product.get("product_name", ""))

#         if product_name and search_name in product_name.lower():

#             brand = product.get("brands", "")
#             category = product.get("categories", "")
#             ingredients = extract_ingredients(product.get("ingredients_text", ""))

#             print(f'brand: "{brand}"')
#             print(f'category: "{category}"')
#             print("ingredients:")
#             for idx, ing in enumerate(ingredients):
#                 print(f'  {idx} "{ing}"')
#             print(f'product_name: "{product_name}"')

#             return

#     print("Product not found")


# # ---- RUN ----
# fetch_product_by_name("Oreo")





import csv
import re
import time
from datasets import load_dataset
from tqdm import tqdm

# ---------------- LOAD DATASET ----------------
dataset = load_dataset(
    "openfoodfacts/product-database",
    split="food",
    streaming=True
)

# ---------------- PRODUCT LISTS ----------------
BISCUITS = [
    "Oreo Original", "Good Day Butter", "Marie Gold", "Parle-G",
    "Hide & Seek Fab", "Bourbon", "Treat Croissant", "Milk Bikis",
    "Little Hearts", "50-50 Biscuit", "Nice Time", "Monaco",
    "Dark Fantasy Choco Fills", "Tiger Crunch", "NutriChoice Digestive",
    "Jim Jam", "Milano Cookies", "Nice Cream Biscuit",
    "Marie Light", "Good Day Cashew", "Hide & Seek Black Bourbon",
    "Little Hearts Strawberry", "Milk Bikis Cream",
    "Treat Oreo", "Krackjack", "Marie Gold Cream",
    "Good Day Choco Chip", "50-50 Maska Chaska",
    "Milk Bikis Atta", "Parle-G Gold"
]

CANDIES = [
    "Alpenliebe Creamfills", "Pulse Candy", "Eclairs Plus",
    "Mentos Mint", "Mentos Fruit", "Kaccha Mango Bite",
    "Kopiko Coffee Candy", "Lollipop Chupa Chups",
    "Center Fresh", "Center Fruit", "Melody Chocolate Candy",
    "Poppins", "Fruittella", "Lacto King",
    "Candyman Toffee", "Big Babool", "Rola Cola",
    "Orange Bite", "Coffee Bite", "Alpenliebe Gold",
    "Boomer Bubble Gum", "XOXO Lollipop",
    "Pulse Kachcha Aam", "Alpenliebe Juzt Jelly",
    "Mentos Strawberry", "Mentos Lemon",
    "Candyman Fantastik", "Poppins Jelly",
    "Choco Eclairs", "Milky Bar Eclairs"
]

CHOCOLATES = [
    "Dairy Milk", "Dairy Milk Silk", "Dairy Milk Fruit & Nut",
    "5 Star", "Perk", "Munch", "KitKat",
    "KitKat Dark", "Snickers", "Snickers Peanut",
    "Mars", "Galaxy Smooth Milk", "Milky Bar",
    "Fuse Chocolate", "Amul Milk Chocolate",
    "Amul Dark Chocolate", "Bournville",
    "Dairy Milk Crackle", "Dairy Milk Lickables",
    "KitKat Chunky", "Toblerone Milk",
    "Ferrero Rocher", "Kinder Joy",
    "Kinder Bueno", "Nestle Classic",
    "Dairy Milk Roast Almond",
    "Dairy Milk Silk Oreo", "Dairy Milk Silk Hazelnut",
    "KitKat White", "Milkybar Moosha"
]

CHIPS = [
    "Lay's Classic Salted", "Lay's Magic Masala",
    "Lay's Cream & Onion", "Lay's Chile Limon",
    "Bingo Mad Angles", "Bingo Tedhe Medhe",
    "Kurkure Masala Munch", "Kurkure Chilli Chatka",
    "Uncle Chipps", "Uncle Chipps Spicy Treat",
    "Pringles Original", "Pringles Sour Cream & Onion",
    "Pringles Texas BBQ", "Balaji Masala Masti",
    "Balaji Simply Salted", "Too Yumm Chips",
    "Too Yumm Multigrain", "Haldiram's Aloo Bhujia",
    "Haldiram's Classic Chips", "Haldiram's Cream & Onion",
    "Lay's West Indies Hot n Sweet Chilli",
    "Lay's American Style Cream & Onion",
    "Lay's India's Magic Masala",
    "Bingo Yumitos", "Bingo Original Style",
    "Kettle Studio Sea Salt",
    "Kettle Studio Jalapeno",
    "Kettle Studio Himalayan Salt",
    "TagZ Chips", "Cornitos Nacho Crisps"
]

# ---------------- HELPERS ----------------
def clean_html(text):
    return re.sub(r"<.*?>", "", text)

def extract_text(data, preferred_lang="en"):
    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict) and item.get("lang") == preferred_lang:
                return item.get("text", "")
        for item in data:
            if isinstance(item, dict) and "text" in item:
                return item["text"]
    if isinstance(data, str):
        return data
    return ""

def extract_ingredients(raw):
    text = clean_html(extract_text(raw))
    if not text:
        return ""
    return ", ".join([i.strip() for i in text.split(",") if i.strip()])

def fetch_product(product_name, fallback_category):
    for product in dataset:
        name = extract_text(product.get("product_name", ""))
        if name and product_name.lower() in name.lower():
            brand = product.get("brands", "")
            category = product.get("categories", "")
            if not category or category.lower() == "undefined":
                category = fallback_category
            ingredients = extract_ingredients(product.get("ingredients_text", ""))
            return [name, brand, category, ingredients]

    return [product_name, "", fallback_category, ""]

# ---------------- CSV BUILDER WITH PROGRESS ----------------
def build_csv(filename, product_list, category):
    start = time.time()

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["product_name", "brand", "category", "ingredients"])

        for p in tqdm(product_list, desc=f"Creating {filename}", unit="product"):
            row = fetch_product(p, category)
            writer.writerow(row)

    elapsed = time.time() - start
    print(f"{filename} created in {elapsed:.2f} seconds\n")

# ---------------- RUN ----------------
overall_start = time.time()

build_csv("biscuits.csv", BISCUITS, "Biscuit")
build_csv("candies.csv", CANDIES, "Candy")
build_csv("chocolates.csv", CHOCOLATES, "Chocolate Bar")
build_csv("chips.csv", CHIPS, "Chips")

total_time = time.time() - overall_start
print(f"✅ All files created successfully in {total_time:.2f} seconds")

