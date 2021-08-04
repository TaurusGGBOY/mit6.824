for ((a=1;a<=100;a++))
do
 echo ".....index =$a"
 time go test -run TestFigure82C -race> ./test/report_$a
 s=$(tail -n 1 ./test/report_$a)
 #echo ${s:0:2}
 #echo "ok"
 if [ ${s:0:2} == 'ok' ]
 then

  echo "test $a Passed"
#   rm ./test/report_$a
 else
  echo "test $a Failed"
 fi
done
