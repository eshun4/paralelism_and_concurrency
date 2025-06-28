import asyncio


async def async_sleep():
    print("Before sleep")
    await asyncio.sleep(5)
    print("After sleep")
    
async def print_hello():
   print("hello") 

async def main():
    await async_sleep()

    await print_hello()
    
if __name__ == '__main__':
    asyncio.run(main())