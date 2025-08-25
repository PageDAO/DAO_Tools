"""Lightweight setup wrapper.

This file intentionally avoids importing setuptools at module import time so that
the Streamlit runtime won't attempt to import it when executing the app.
"""

def main():
    from setuptools import setup, find_packages

    setup(
        name="dao_ledger",
        version="0.1.0",
        description="Streamlit application to generate DAO accounting reports from proposal data.",
        packages=find_packages(where="DaoLedger"),
        package_dir={"": "DaoLedger"},
        include_package_data=True,
        install_requires=[
            "pandas>=2.3.2",
            "plotly>=6.3.0",
            "requests>=2.32.5",
            "streamlit>=1.48.1",
            "reportlab>=4.4.3",
            "kaleido>=0.2.1",
        ],
    )


if __name__ == "__main__":
    main()
