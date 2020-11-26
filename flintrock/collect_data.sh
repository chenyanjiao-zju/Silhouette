
# global
result_file="result.csv"

for file in ./*
do
    if test -f $file -a ${file##*.} = 'py'
    then
        echo $file
        cat ${file} | awk -F: '{sum+=$2} END {print sum/NR}'
    fi
done

# compress those data
# tar -cvzf `date +%F-%H-%M`-{$1}.tar.gz *py
# delete all data
# rm *py






# global
result_file="result.csv"

for file in ./*
do
    if test -f $file -a ${file##*.} = 'py'
    then
        echo ${file##*/}
        arr=(`echo ${file##*/} | tr '-' ' '`)
        cores=${arr[0]}
        part=${arr[1]}
        res=`cat ${file} | awk -F: '{sum+=$2} END {print sum/NR}'`
        echo "${cores},${part},${res}" >> $result_file
    fi
done

# compress those data
tar -cvzf `date +%F-%H-%M`-{$1}.tar.gz *py
# delete all data
rm *py