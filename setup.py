"""No-op setup shim to avoid side effects during runtime.

This file intentionally contains no import-time actions. It exists only so
the repository doesn't expose an executable setup.py to the PaaS runtime.
"""

def main():
    # Intentionally empty
    return None


if __name__ == "__main__":
    main()
