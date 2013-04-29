for foo in flight_id metric tb_h feature base_feature metric_feature cnt; do
echo $foo;
/home/maphysics/.rbenv/shims/ruby /home/maphysics/GitProjects/wonderdog/test/getFields.rb --dump=/home/maphysics/GitProjects/wonderdog/test/flight_count_20130405 --field=$foo >> $foo.txt ;
cat $foo.txt |sort | uniq -c |sort -n | wc -l;
done
