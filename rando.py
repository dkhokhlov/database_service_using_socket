import random
import argparse
import json

OCCUPATIONS = ["Lumberjack", "Blacksmith", "Accountant", "Carpenter", "Soldier", "Engineer"]

FIRST_NAMES = ["Mike", "Kelly", "Brennan", "Jeff", "Matthew", "Hugh", "Ryan"]

LAST_NAMES = ["Fisher", "Grossman", "Cruse", "Ericson", "Buffett", "Smith", "Stewart"]

TEMPLATE = """
{{
    "first_name":"{}",
    "last_name":"{}",
    "occupation":"{}",
    "SSN":"900-299-{}",
    "DOB":"{}-{}-{}"
}}  
"""


def main():
    parser = argparse.ArgumentParser(description="generate random users")
    parser.add_argument("--outfile", help="output file")
    parser.add_argument("--num", default=1, type=int, help="Number of random users to create")
    args = parser.parse_args()

    items = [json.loads(rando()) for i in range(args.num)]

    if args.outfile:
        with open(args.outfile, "w") as f:
            json.dump(items, f, indent=2)
    else:
        print(json.dumps(items, indent=2))


def rando() -> str:
    return TEMPLATE.format(
        FIRST_NAMES[random.randrange(len(FIRST_NAMES))],
        LAST_NAMES[random.randrange(len(LAST_NAMES))],
        OCCUPATIONS[random.randrange(len(OCCUPATIONS))],
        random.randrange(9999),
        random.randrange(1, 12),
        random.randrange(1, 28),
        random.randrange(1800, 2000),
    )


if __name__ == "__main__":
    main()
