#!/bin/bash
Help()
{
   echo
   echo "Script para verificar uso de RAM."
   echo
   echo "Sintaxe: mem [-h|-n nome_do_programa]"
   echo "Opcoes:"
   echo "-h                     Imprime essa mensagem."
   echo "-n nome_do_programa    Busca programa pelo nome especificado."
   echo
   exit 1
}
while getopts "h:n:" opt
do
   case "$opt" in
      n ) parameterN="$OPTARG" ;;
      ? ) Help ;;
   esac
done
if [ -z "$parameterN" ]
then
   Help
fi
ps -eo size,pid,user,command --sort -size | awk '{ hr=$1/1024 ; printf("PID:%d    MEM:%13.2f Mb    ",$2,hr) } { for ( x=4 ; x<=NF ; x++ ) { printf("%s ",$x)
 } print "" }' |    cut -d "" -f2 | cut -d "-" -f1 | grep $parameterN
echo
echo "Memoria livre:"
echo
free -h