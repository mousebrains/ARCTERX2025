# How to set up NGINX on Pis

- `sudo cp arcterx.nginx /etc/nginx/sites-available/arcterx`
- `cd /etc/nginx/sites-enabled`
- `sudo ln -s ../sites-available/arcterx`
- `sudo rm default`
- `cd ..`
- Edit nginx.conf and change all instances of www-data to pat
- Edit /etc/php/8.3/fpm/pool.d/www.conf and change all instances of www-data to pat and change pm.max_children to 10
- mkdir ~pat/public_html
- `sudo systemctl restart php8.3-fpm.service nginx`

