import os
import signal
from dotenv import load_dotenv
import subprocess
from aiogram import Bot, Dispatcher, types, filters


load_dotenv()
BOT_TOKEN = os.getenv('BOT_TOKEN')
INPUT_DATA_PATH = os.getenv('INPUT_DATA_PATH')
PATH_TO_SCRIPT = os.getenv('PATH_TO_SCRIPT')


bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()


@dp.message()
async def main(message: types.Message):
    input_json = message.text
    try:
        if (message.text.startswith('/start') is not True):
            print(message.text)

            script_dir = os.path.dirname(os.path.abspath(__file__))
            full_path = os.path.join(script_dir, INPUT_DATA_PATH)

            with open(full_path, "w") as file:
                file.write(input_json)

            result = subprocess.run(['python3', PATH_TO_SCRIPT],
                                    capture_output=True, text=True)
            if result.returncode == 0:
                answer = result.stdout
            else:
                answer = f'{ result.returncode}: {result.stderr}'

            await message.answer(answer)
    except Exception as e:
        await message.reply(f"Invalid input\n{e}")


def signal_handler(signum, frame):
    bot.close()
    dp.stop_polling()
    exit(0)


if __name__ == '__main__':
    try:
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)

        dp.run_polling(bot)
    except Exception as e:
        print(e)
        exit(1)
