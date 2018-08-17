FROM srp33/wishbuilder-cli-environment

WORKDIR /WishBuilder-CLI

RUN cd /tmp; git clone https://github.com/srp33/ShapeShifter.git; mv ShapeShifter/ShapeShifter /
ADD LICENSE /WishBuilder-CLI/
ADD README.md /WishBuilder-CLI/
ADD *.py /WishBuilder-CLI/

RUN chmod 777 /WishBuilder-CLI -R

CMD ["python3", "WishBuilder.py"]
