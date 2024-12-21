import random
i = 0
x = set()
y = set()
with open("example_graph.txt", "w") as file:
    while i < 100000:
        random_number1 = random.randint(1, 100000)
        random_number2 = random.randint(1, 100000)
        if(random_number1 == random_number2):
            continue
        if (random_number1,random_number2) in x or (random_number2,random_number1) in x:
            continue
        x.add((random_number1 , random_number2))
        y.add(random_number2)
        y.add(random_number1)
        file.write(f"{random_number1}    {random_number2}\n")
        i += 1
print(len(y))