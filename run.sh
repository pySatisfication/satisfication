
if [ $1 == "start" ];then
    redis-cli set all_valid_ct IF2209,IF2207,IF2212,IF2208,IH2209,IH2207,IH2212,IH2208,IC2209,IC2207,IC2212,IC2208,rb2207,rb2212,rb2211,rb2210,rb2208,rb2209,rb2301,rb2302,rb2303,rb2306,rb2304,rb2305,hc2207,hc2212,hc2211,hc2210,hc2208,hc2209,hc2301,hc2302,hc2303,hc2306,hc2304,hc2305,fu2207,fu2301,fu2211,fu2212,fu2209,fu2208,fu2210,fu2303,fu2302,fu2304,fu2306,fu2305,bu2209,bu2303,bu2212,bu2306,bu2312,bu2309,bu2207,bu2208,bu2210,bu2211,bu2301,bu2302,bu2403,bu2406,bu2304,bu2305,sp2207,sp2212,sp2211,sp2210,sp2208,sp2209,sp2301,sp2302,sp2303,sp2306,sp2304,sp2305,sc2209,sc2309,sc2212,sc2303,sc2306,sc2403,sc2312,sc2207,sc2406,sc2301,sc2211,sc2412,sc2208,sc2210,sc2409,sc2503,sc2302,sc2304,sc2506,sc2305,m2207,m2211,m2212,m2209,m2208,m2301,m2303,m2305,y2207,y2211,y2212,y2209,y2208,y2301,y2303,y2305,p2207,p2211,p2212,p2209,p2208,p2210,p2301,p2303,p2302,p2306,p2305,p2304,i2207,i2211,i2212,i2209,i2208,i2210,i2301,i2303,i2302,i2306,i2305,i2304,j2207,j2211,j2212,j2209,j2208,j2210,j2301,j2303,j2302,j2306,j2305,j2304,jm2207,jm2211,jm2212,jm2209,jm2208,jm2210,jm2301,jm2303,jm2302,jm2306,jm2305,jm2304,v2207,v2211,v2212,v2209,v2208,v2210,v2301,v2303,v2302,v2306,v2305,v2304,SR207,SR211,SR209,SR301,SR303,SR305,CF207,CF211,CF209,CF301,CF303,CF305,TA207,TA211,TA212,TA209,TA208,TA210,TA301,TA303,TA302,TA306,TA305,TA304,MA207,MA211,MA212,MA209,MA208,MA210,MA301,MA303,MA302,MA306,MA305,MA304,SA207,SA211,SA212,SA209,SA208,SA210,SA301,SA303,SA302,SA306,SA305,SA304,FG207,FG211,FG212,FG209,FG208,FG210,FG301,FG303,FG302,FG306,FG305,FG304
fi

cd depth_server
sh service_kline.sh $1

sleep 1m

cd ../pack_service
sh service_pack.sh $1

cd ..

