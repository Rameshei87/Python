print("Welcome to DBS Bank")

# press 1 for Balance
# press 2 for withdraw
restart = 'Y'
balance = 100.05
chance = 3
while chance >= 0:
    pin = int(input("Enter the PIN Number : "))
    if pin == 1234:
        print("::: Access Granted :::")
        print("Press 1 to see the balance : \n")
        print("Press 2 to withdraw the amount : \n")
        option = int(input("Enter the Option : "))
        if option == 1:
            print("Your Account Balance is : Rs", balance)
            restart = input("Would you like to continue ?")
            if restart in ('n', 'N', 'no', 'NO'):
                print("Thank you")
                break
        elif option == 2:
            withdraw = float(input("Please select the amount you want to withdraw "))
            if withdraw in [50, 100]:
                balance = balance - withdraw
                print("Your Balance after withdraw : ", balance)
                restart = input("Would you like to continue ?")
                if restart in ('n', 'N', 'no', 'NO'):
                    print("Thank you")
                    break
            elif withdraw !=[10,50,100]:
                print("Invalid Amount \n")
                restart= 'Y'
    elif pin != 1234:
        print("incorrect Password")
        chance = chance - 1
        print("Still you have ", chance, ' More chance to login')
        if chance == 0:
            print("Exceeds more than 3 times ")
            break
