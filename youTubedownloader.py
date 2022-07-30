from pytube import YouTube

link=input('Enter the link of Video :')
ytd =YouTube(link)

#print(ytd)
# print(ytd.title)
# print(ytd.thumbnail_url)
# print(ytd.streams.all())
videos=ytd.streams.all()
# stream=ytd.streams.first()
# stream.download()
print(videos)
print("Video Downloaded Successfully")