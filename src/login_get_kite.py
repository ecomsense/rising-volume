import pickle
from toolkit.fileutils import Fileutils
from stock_brokers.zerodha.zerodha import Zerodha
from stock_brokers.bypass.bypass import Bypass
from constants import S_DATA, logging
from toolkit.kokoo import timer


def get_kite(**kwargs):
    try:
        if kwargs.get("api_type") == "bypass":
            kite = get_bypass(**kwargs)
        else:
            kite = get_zerodha(**kwargs)
        return kite
    except Exception as e:
        logging.error(f"{e} while getting kite obj")


def get_bypass(**kwargs):
    try:
        tokpath = f"{S_DATA}{kwargs['userid']}{'.txt'}"
        pklpath = f"{S_DATA}{kwargs['userid']}{'.pkl'}"
        enctoken = None
        f = Fileutils()
        if f.is_file_not_2day(tokpath) is False:
            print(f"file modified today ... reading {enctoken}")
            with open(tokpath, "r") as tf:
                enctoken = tf.read()
                print(f"enctoken sent to broker {enctoken}")
        bypass = Bypass(
            userid=kwargs["userid"],
            password=kwargs["password"],
            totp=kwargs["totp"],
            tokpath=tokpath,
            enctoken=enctoken,
        )
        if bypass.authenticate():
            if not enctoken:
                enctoken = bypass.kite.enctoken
            if enctoken:
                with open(tokpath, "w") as tw:
                    print("writing enctoken to file")
                    tw.write(enctoken)
                with open(pklpath, "wb") as pkl:
                    pickle.dump(bypass, pkl)
            else:
                print(
                    f"Not able to get or generate enctoken for {bypass.userid}, check your credentials..."
                )
    except Exception as e:
        print(f"unable to create bypass object {e}")
    else:
        return bypass


def get_zerodha(**kwargs):
    try:
        kwargs["tokpath"] = f"{S_DATA}{kwargs['userid']}{'.txt'}"
        kite = Zerodha(
            userid=kwargs["userid"],
            password=kwargs["password"],
            totp=kwargs["totp"],
            api_key=kwargs["api_key"],
            secret=kwargs["secret"],
        )
        kite.authenticate()
        # kite.enctoken = ""
        return kite
    except Exception as e:
        print(f"exception while creating zerodha object {e}")
        timer(2)
        print("trying to log in again")
        get_zerodha(**kwargs)


if __name__ == "__main__":
    from constants import O_CNFG

    kobj = get_kite(**O_CNFG)
    print(kobj)
