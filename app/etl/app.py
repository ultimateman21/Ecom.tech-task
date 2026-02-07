import sys

from generator import main as generate_main
from upload_exist import main as upload_main
from process import main as process_main
from view import main as view_main


MENU = {
    '1': ('Generate datasets and upload to MinIO', generate_main),
    '2': ('Upload existing parquet datasets', upload_main),
    '3': ('Run ETL pipeline', process_main),
    '4': ('View result dataset', view_main),
    '0': ('Exit', None),
}


def print_menu():
    print("\n" + "=" * 50)
    print("ETL Console Menu")
    print("=" * 50)

    for key, (title, _) in MENU.items():
        print(f"{key}. {title}")

    print("=" * 50)


def main():
    while True:
        print_menu()
        choice = input('Select action: ').strip()

        if choice not in MENU:
            print('Invalid choice, try again.')
            continue

        title, action = MENU[choice]

        if choice == '0':
            print('Exit.')
            sys.exit(0)

        try:
            action()
        except Exception as e:
            print(f'\nERROR: {e}')

        input('\nPress Enter to continue...')


if __name__ == "__main__":
    main()
