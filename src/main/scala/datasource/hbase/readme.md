### **创建hbase表**

    create 'tblStudent','Grade','Info'

    scan 'tblStudent'

    put 'tblStudent','001','Info:age','24'
    put 'tblStudent','001','Info:name','xiaoming'
    put 'tblStudent','001','Grade:math','80'
    put 'tblStudent','001','Grade:chinese','75'
    put 'tblStudent','001','Grade:english','92'


    put 'tblStudent','002','Info:age','22'
    put 'tblStudent','002','Info:name','xiaohong'
    put 'tblStudent','002','Grade:math','88'
    put 'tblStudent','002','Grade:chinese','90'
    put 'tblStudent','002','Grade:english','99'
    
    put 'tblStudent','003','Info:age','222'
    put 'tblStudent','003','Info:name','haha'
    put 'tblStudent','003','Grade:math','85'
    put 'tblStudent','003','Grade:chinese','4'
    put 'tblStudent','003','Grade:english','0'
    
    put 'tblStudent','004','Info:age','111'
    put 'tblStudent','004','Info:name','uuuu'
    put 'tblStudent','004','Grade:math','35'
    put 'tblStudent','004','Grade:chinese','66'
    put 'tblStudent','004','Grade:english','8'