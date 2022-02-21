package main

import (
	"context"
	"fmt"
	"gRPC-GO/blog/blogpb"
	"io"
	"log"

	"google.golang.org/grpc"
)

func main() {
	fmt.Println("Blog client")

	opts := grpc.WithInsecure()

	cc, err := grpc.Dial("localhost:50051", opts)
	if err != nil {
		log.Fatalf("couldn't connect: %v", err)
	}

	defer cc.Close()

	c := blogpb.NewBlogServiceClient(cc)

	// create Blog
	fmt.Println("Creating blog")
	blog := &blogpb.Blog{
		AuthorId: "Adem",
		Title:    "First blog",
		Content:  "gRPC + golang API",
	}

	createdBlogRes, err := c.CreateBlog(context.Background(), &blogpb.CreateBlogRequest{Blog: blog})
	if err != nil {
		log.Fatalf("Unexpected error: %v", err)
	}
	fmt.Printf("Blog Created: %v\n", createdBlogRes)
	blogId := createdBlogRes.GetBlog().GetId()

	// read blog
	fmt.Printf("Reading the blog: %v\n", blogId)

	_, err2 := c.ReadBlog(context.Background(), &blogpb.ReadBlogRequest{BlogId: "1234"})
	if err2 != nil {
		fmt.Printf("Error happened while reading: %v\n", err2)
	}

	readBlogReq := &blogpb.ReadBlogRequest{BlogId: blogId}
	readBlogRes, readBlogErr := c.ReadBlog(context.Background(), readBlogReq)
	if readBlogErr != nil {
		fmt.Printf("Error happened while reading: %v\n", readBlogErr)
	}
	fmt.Printf("Blog was read: %v\n", readBlogRes)

	// update blog
	newBlog := &blogpb.Blog{
		Id:       blogId,
		AuthorId: "Adem changed",
		Title:    "First blog(edited)",
		Content:  "gRPC + golang API 12345678",
	}
	updateRes, updateErr := c.UpdateBlog(context.Background(), &blogpb.UpdateBlogRequest{Blog: newBlog})
	if updateErr != nil {
		fmt.Printf("Error happened while updating: %v \n", updateErr)
	}
	fmt.Printf("Blog was updated: %v\n", updateRes)

	// delete blog
	deleteRes, deleteErr := c.DeleteBlog(context.Background(), &blogpb.DeleteBlogRequest{BlogId: blogId})
	if deleteErr != nil {
		fmt.Printf("Error happened while deleting: %v \n", deleteErr)
	}
	fmt.Printf("Blog was deleted: %v\n", deleteRes)

	// list blogs

	stream, listErr := c.ListBlog(context.Background(), &blogpb.ListBlogRequest{})

	if listErr != nil {
		log.Fatalf("error while calling ListBlog RPC: %v", err)
	}

	for {

		msg, err := stream.Recv()
		if err == io.EOF {
			//we've reached the end of the stream
			break
		}
		if err != nil {
			log.Fatalf("error while reading the stream: %v", err)
		}
		log.Println(msg.GetBlogs())
	}
}
