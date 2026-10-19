"""Check the Linux browser required by the IPBX report endpoints."""
from playwright.sync_api import sync_playwright

with sync_playwright() as playwright:
    browser = playwright.chromium.launch(headless=True)
    page = browser.new_page()
    page.goto("about:blank")
    assert page.title() == ""
    browser.close()
print("Chromium headless OK")
