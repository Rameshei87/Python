#Floyds Triangle

num=int(input("Enter the number of rows :"))
k=1
for i in range(k, num+1): #1 to user input value(-1)
    for j in range(1,i+1):
        print(k, end=" ")
        k=k+1
    print()
