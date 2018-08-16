FROM srp33/wishbuilder-cli-environment

WORKDIR /app

ADD *.py /app/
ADD LICENSE /app/
ADD README.md /app/

CMD ["python3", "WishBuilder.py"]
