import pikepdf
from tqdm import tqdm

# load password list
passwords = []
for line in open("rockyou.txt", encoding="utf-8"):
    passwords.append(line.strip())

# Combinations to try
n_words = len(passwords)
print("Total Combinations of passwords to try : ", n_words)

# iterate over passwords
for password in tqdm(passwords, "Decrypting PDF"):
    try:
        # open PDF file
        with pikepdf.open("sample_protected.pdf", password=password) as pdf:
            # Password decrypted successfully, break out of the loop
            print("[+] Password found:", password)
            break
    except pikepdf._qpdf.PasswordError as e:
        # wrong password, just continue in the loop
        continue
