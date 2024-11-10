# How I set up a Raspberry Pi for use on the ships of the ARCTERX pilot cruise in 2022
---
1. Create MicroSD card with Ubuntu server for a Raspberry Pi system. 
  - I'm using 128GB or 256GB cards.
  - I installed Ubuntu 21.10 64 bit server. 
  - To burn the MicroSD card I used the "Raspberry Pi Imager" application on my MacOS desktop.
2. Install the MicroSD in the Pi
3. Boot the Pi and get the IP address.
4. Log into the Pi `ssh ubuntu@ipAddr` where ipAddr is the IP address you foudn in the previous step.
5. The initial password is *ubuntu* which you'll have to change on the first login.
6. Log back into the Pi using the new password, `ssh ubuntu@ipAddr`
7. Set the hostname using the command `sudo hostnamectl set-hostname YourNewHostname`
8. Enable zeroconf/bonjour via AVAHI-DAEMON, `sudo apt install avahi-daemon --yes`
9. Add the primary user via the commands
  - `sudo adduser pat`
  - `sudo usermod -aG sudo pat`
10. Logout
11. Copy your *SSH* keys to the Pi using `ssh-copy-id pat@YourNewHostname.local` where YourNewHostname is what you set above.
12. Log into the new user via `ssh pat@YourNewHostname.local` where pat is the username you used above.
13. Check that the new user has *sudo* privledges `sudo ls -la`
14. Delete the ubuntu user, `sudo deluser --remove-home ubuntu`
15. Clone the git repository `git clone --recurse-submodules git@github.com:mousebrains/ARCTERX.git`
16. `cd ARCTERX/Setup.pi4`
17. `install0.py --verbose` This will do the following:
  - `sudo apt update`
  - `sudo apt upgrade`
  - `sudo apt autoremove`
  - Disable autoupgrades
  - `git config --global user.name "Pat Welch"`
  - `git config --global user.email pat@mousebrains.com`
  - `git config --global core.editor vim`
  - `git config --global pull.rebase false`
  - `git config --global submodule.recurse true`
  - `git config --global diff.submodule log`
  - `git config --global status.submodulesummary 1`
  - `git config --global push.recurseSubmodules on-demand`
  - `sudo apt install fail2ban`
  - `sudo apt install nginx`
  - `sudo apt install php-fpm`
  - `sudo apt install samba*`
  - `sudo apt install php-xml`
  - `sudo apt install php-yaml`
  - `sudo apt install python3-pip`
  - `sudo apt install python3-pandas`
  - `sudo apt install python3-xarray`
  - `sudo apt install python3-geopandas`
  - `python3 -m pip install --user inotify-simple`
  - `python3 -m pip install --user libais`
  - Update ~/.ssh/config for arcterx
  - Create ~/.ssh/id_rsa, if needed
  - Add new id_rsa key to arcterx authorized_keys
  - Set up reverse ssh tunnel
  - Set up syncthing
  - Set up samba mount point
  - `sudo reboot`

31. Add a mount points for all users on all machines by adding *~/SUNRISE/SAMBA/forall.conf* to */etc/samba/smb.conf*
32. If on the waltonsmith0, create a mount point for Jasmine and the ASVs by:
  - Create the mount point if it does not exist for ASVs `mkdir /home/pat/Dropbox/WaltonSmith/asv`
  - Add *~/SUNRISE/SAMBA/ws.conf* to */etc/samba/smb.conf*
  - Create the SMB user with a known password, Sunrise, `sudo smbpasswd -a pat`
35. Restart samba, `sudo systemctl restart smbd`
36. Install the required PHP packages, `sudo apt install php-xml php-yaml`
38. Set up git
  - `cd ~`
  - `git config --global user.name "Pat Welch"`
  - `git config --global user.email pat@mousebrains.com`
  - `git config --global core.editor vim`
  - `git config --global pull.rebase false`
  - `git clone git@github.com:mousebrains/SUNRISE.git`
39. Modify the webserver configuration to point hat /home/pat/Dropbox and to run as user pat.
 - `cd ~/SUNRISE/nginx`
 - `make`
39. Install services using hostname discover method
  - `cd ~/SUNRISE`
  - `sudo ./install.services.py --discover`
40. Install the primary index.php
  - `cd ~/SUNRISE/html`
  - `make install`
44. To add a user use `~/SUNRISE/addUser` this sets up the user and group memberships
---
# For ship ops undo the shore side testing:
- `sudo rm /etc/ssh/sshd_config.d/osu_security.conf`
- `sudo rm /etc/ssh/banner.txt`
- `sudo rm /etc/motd`
- `sudo systemctl restart sshd`
- `sudo systemctl disable fail2ban`
- `sudo systemctl stop fail2ban`
