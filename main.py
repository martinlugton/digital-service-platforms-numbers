import requests
from bs4 import BeautifulSoup
import json
from google.cloud import storage
import logging

def scrape_pay(url):
    logging.info(f"Scraping GOV.UK Pay from: {url}")
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors
        logging.info(f"Raw HTML for GOV.UK Pay (first 500 chars): {response.text[:500]}")
        soup = BeautifulSoup(response.content, "html.parser")
        
        data = {}
        # Live services
        live_services_element = soup.select_one('div.govuk-grid-row:nth-of-type(3) > div:nth-child(1) > div.govuk-heading-l')
        if live_services_element:
            data['live_services'] = live_services_element.text.strip()

        # Total transactions processed
        transactions_processed_element = soup.select_one('div.govuk-grid-row:nth-of-type(3) > div:nth-child(2) > div.govuk-heading-l')
        if transactions_processed_element:
            data['transactions_processed'] = transactions_processed_element.text.strip()

        # Total amount processed
        total_amount_element = soup.select_one('div.govuk-grid-row:nth-of-type(3) > div:nth-child(3) > div.govuk-heading-l')
        if total_amount_element:
            data['total_amount'] = total_amount_element.text.strip()

        # Number of organisations (using new logic)
        organisations_h2 = soup.find('h2', string="Organisations using GOV.UK Pay")
        if organisations_h2:
            organisations_div = organisations_h2.find_next('div', class_="govuk-heading-l govuk-!-margin-bottom-0")
            if organisations_div:
                data['organisations'] = organisations_div.text.strip()
            else:
                logging.warning("Could not find the target div after 'Organisations using GOV.UK Pay' H2 tag.")
        else:
            logging.warning("Could not find 'Organisations using GOV.UK Pay' H2 tag.")

        logging.info(f"Scraped GOV.UK Pay data: {data}")
        return data
    except requests.exceptions.RequestException as e:
        logging.error(f"HTTP Request failed for GOV.UK Pay: {e}")
        return {}
    except Exception as e:
        logging.error(f"Error parsing GOV.UK Pay data: {e}")
        return {}

def scrape_notify(url):
    logging.info(f"Scraping GOV.UK Notify from: {url}")
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors
        logging.info(f"Raw HTML for GOV.UK Notify (first 500 chars): {response.text[:500]}")
        soup = BeautifulSoup(response.content, "html.parser")

        data = {}

        # Messages sent since May 2016
        messages_sent_element = soup.find('h2', id='messages-sent-since-may-2016')
        if messages_sent_element:
            messages_value = messages_sent_element.find_next_sibling('div', class_='totals').find('span', class_='product-page-big-number')
            if messages_value:
                data['messages_sent'] = messages_value.text.strip()

        # Number of organisations and services
        organisations_h2 = soup.find('h2', id='organisations-using-notify')
        if organisations_h2:
            organisations_p = organisations_h2.find_next_sibling('p', class_='totals--2-column')
            if organisations_p:
                organisations_value = organisations_p.find('span', class_='totals__all').find('span', class_='product-page-big-number')
                if organisations_value:
                    data['organisations'] = organisations_value.text.strip()
                services_value = organisations_p.find('span', class_='totals__set-type').find('span', class_='product-page-big-number')
                if services_value:
                    data['services'] = services_value.text.strip()

        logging.info(f"Scraped GOV.UK Notify data: {data}")
        return data
    except requests.exceptions.RequestException as e:
        logging.error(f"HTTP Request failed for GOV.UK Notify: {e}")
        return {}
    except Exception as e:
        logging.error(f"Error parsing GOV.UK Notify data: {e}")
        return {}

def scrape_forms(url):
    logging.info(f"Scraping GOV.UK Forms from: {url}")
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors
        logging.info(f"Raw HTML for GOV.UK Forms (first 500 chars): {response.text[:500]}")
        soup = BeautifulSoup(response.content, "html.parser")
        
        data = {}
        # Published forms
        published_forms_div = None
        for div in soup.find_all('div', class_='app-metrics__big-number'):
            if "published forms" in div.get_text():
                published_forms_div = div
                break

        if published_forms_div:
            published_forms_span = published_forms_div.find('span', class_='app-metrics__big-number-number')
            if published_forms_span:
                data['published_forms'] = published_forms_span.text.strip()
            else:
                logging.warning("Could not find 'app-metrics__big-number-number' span within the 'published forms' div.")
        else:
            logging.warning("Could not find 'app-metrics__big-number' div containing 'published forms' text.")

        # Form submissions
        form_submissions_divs = soup.find_all('div', class_='app-metrics__big-number')
        if form_submissions_divs:
            form_submissions_element = form_submissions_divs[-1].find('span', class_='app-metrics__big-number-number')
            if form_submissions_element:
                data['form_submissions'] = form_submissions_element.text.strip()
            else:
                logging.warning("Could not find 'app-metrics__big-number-number' span within the last 'app-metrics__big-number' div.")
        else:
            logging.warning("Could not find any 'app-metrics__big-number' divs for form submissions.")
        logging.info(f"Scraped GOV.UK Forms data: {data}")
        return data
    except requests.exceptions.RequestException as e:
        logging.error(f"HTTP Request failed for GOV.UK Forms: {e}")
        return {}
    except Exception as e:
        logging.error(f"Error parsing GOV.UK Forms data: {e}")
        return {}

def main(request):
    logging.info("Cloud Function main function started.")

    pay_url = "https://www.payments.service.gov.uk/performance/"
    notify_url = "https://www.notifications.service.gov.uk/features/performance"
    forms_url = "https://www.forms.service.gov.uk/performance"

    data = {
        "govuk_pay": scrape_pay(pay_url),
        "govuk_notify": scrape_notify(notify_url),
        "govuk_forms": scrape_forms(forms_url),
    }
    logging.info(f"Combined scraped data: {json.dumps(data, indent=2)}")

    # Upload data to Google Cloud Storage
    try:
        client = storage.Client()
        bucket = client.get_bucket("dsp-numbers-bucket")
        blob = bucket.blob("data.json")
        blob.upload_from_string(json.dumps(data, indent=2), content_type="application/json")
        logging.info("Data successfully uploaded to dsp-numbers-bucket/data.json")
        return "OK"
    except Exception as e:
        logging.error(f"Error uploading data to GCS: {e}")
        return f"Error: {e}", 500