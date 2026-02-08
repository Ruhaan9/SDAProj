from dashboard import dashboard
import sys

def main():
    print("Analysis:")

    try:
        dashboard()
    except KeyboardInterrupt:
        print("Exiting.")
        sys.exit(0)
    except Exception as e:
        print("Unexpected error.")
        sys.exit(1)

if __name__ == "__main__":
    main()
