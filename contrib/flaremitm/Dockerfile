FROM mitmproxy/mitmproxy:latest

RUN pip install --no-cache-dir requests

COPY flaresolverr_addon.py /addons/flaresolverr_addon.py

EXPOSE 8080

CMD ["mitmdump", "--listen-port", "8080", "-s", "/addons/flaresolverr_addon.py"]
