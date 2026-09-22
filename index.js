import http from 'node:http'
import express from 'express'
import path from 'node:path'
import {Server} from 'socket.io'
import {publish,subscribe,redis} from './res-connection.js'

//maintain a state in memory db
//page ke load hote hi in frontend we want the state from backend
const CHECKBOX_SIZE=1_000_000
const CHECKBOX_STATE_KEY='checkbox-state:v2'

const rateLimitHashmap=new Map()
//socket id : 22:02 timestamp




async function main(){
    const PORT=process.env.PORT??8000
    const app=express()
    const server=http.createServer(app)
    const io=new Server();
    io.attach(server)

    await subscribe.subscribe('internal-server:checkbox:change')
    //when message comes we get channel and the actual data
    subscribe.on('message',(channel,message)=>{
        if(channel==='internal-server:checkbox:change'){
            const {index,checked}=JSON.parse(message)
            io.emit('server:checkbox:changed',{index,checked}) //relay to everywhere(jo aaya usse emit kr diya sabko apne apne clients ka)
        }

    })
    //websocket io
    io.on('connection',(socket)=>{
        console.log("SOCKET CONNECTED",{id:socket.id})
        //handle here in backend when some socket gives a data from client side handle it here
        socket.on('client:checkbox:changed',async (data)=>{
            console.log(`[Socket:${socket.id}]:client:checkbox:changed`,data)

            const lastOperationTime=await redis.get(`rate-limiting:${socket.id}`)
            
            if(lastOperationTime){
                const timeElapsed=Date.now()-lastOperationTime
                if(timeElapsed<5*1000){
                    socket.emit('server:error',{
                        error:'Too many requests. Please wait a few seconds.',
                        index:data.index,
                        checked:data.checked
                    })
                    return;
                }

            }
            await redis.set(`rate-limiting:${socket.id}`,Date.now());
        
            


            const existingState=await redis.get(CHECKBOX_STATE_KEY)
            //when socket gives a change we get exiting state from redis then if existing state found then parse it and update and set again in redis 
            //and if no existing state then initially set all to false
            if(existingState){
                const remoteData=JSON.parse(existingState)
                remoteData[data.index]=data.checked
                await redis.set(CHECKBOX_STATE_KEY,JSON.stringify(remoteData))
            }else{ //if there is no existing state
                const initialState=new Array(CHECKBOX_SIZE).fill(false)
                initialState[data.index]=data.checked
                await redis.set(CHECKBOX_STATE_KEY,JSON.stringify(initialState))
                
            }


            //now instead of above code we will relay to redis jo bhi data aaya publsh it to redis
            await publish.publish('internal-server:checkbox:change',JSON.stringify(data))

        })
    })

    app.use(express.static(path.resolve('./public'))) //isme jo bhi file hai wo user ko directly  de de


//express
app.get('/health',(req,res)=>{
    res.json({healthy:true})
})



app.get('/checkboxes',async (req,res)=>{
    const existingState=await redis.get(CHECKBOX_STATE_KEY)
    if(existingState){
        const remoteData=JSON.parse(existingState)
        return res.json({checkboxes:remoteData})

    }
    return res.json({checkboxes:new Array(CHECKBOX_SIZE).fill(false)})

})
server.listen(PORT,()=>{
    console.log(`server is running ${PORT}`)
})

}
main()