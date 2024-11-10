#
# Execute a command via subprocess
#
# March-2022, Pat Welch, pat@mousebrains.com

import logging
import subprocess

def __convertBytes(a:bytes) -> str:
    try:
        return str(a, "utf-8")
    except:
        return a

def execCmd(cmd:tuple[str], check:bool=False, validReturnCodes:tuple[int]=(0,),
        cwd:str=None, inputText:str=None, qIgnoreOutput:bool=False) -> bool:
    ''' Execute cmd and log output '''

    if cwd is None:
        logging.info("Executing %s", " ".join(cmd))
    else:
        logging.info("CWD=%s, Executing %s", cwd, " ".join(cmd))

    s = subprocess.run(cmd, shell=False, 
            input=inputText,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            check=check, cwd=cwd,
            )

    if s.returncode not in validReturnCodes:
        logging.error("Error executing %s", " ".join(cmd))
        if s.stdout:
            logging.error("STDOUT:\n%s", __convertBytes(s.stdout))
        return False

    if not qIgnoreOutput and s.stdout:
        logging.debug("STDOUT:\n%s", __convertBytes(s.stdout))

    return True
