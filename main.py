from src.generator.batch_generator import load_raw_products

def main():
    raw = load_raw_products()
    print(raw["main_category"].unique())

if __name__ == "__main__":
    main()