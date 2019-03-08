//COSC 361 Fall 2018
//FUSE Project Template
//Austin Park

#ifndef __cplusplus
#error "You must compile this using C++"
#endif
#include <fuse.h>
#include <cstdio>
#include <cstdarg>
#include <cstring>
#include <cstdlib>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <fs.h>
#include <vector>

using namespace std;

//Use debugf() and NOT printf() for your messages.
//Uncomment #define DEBUG in block.h if you want messages to show

//Here is a list of error codes you can return for
//the fs_xxx() functions
//
//EPERM          1      /* Operation not permitted */
//ENOENT         2      /* No such file or directory */
//ESRCH          3      /* No such process */
//EINTR          4      /* Interrupted system call */
//EIO            5      /* I/O error */
//ENXIO          6      /* No such device or address */
//ENOMEM        12      /* Out of memory */
//EACCES        13      /* Permission denied */
//EFAULT        14      /* Bad address */
//EBUSY         16      /* Device or resource busy */
//EEXIST        17      /* File exists */
//ENOTDIR       20      /* Not a directory */
//EISDIR        21      /* Is a directory */
//EINVAL        22      /* Invalid argument */
//ENFILE        23      /* File table overflow */
//EMFILE        24      /* Too many open files */
//EFBIG         27      /* File too large */
//ENOSPC        28      /* No space left on device */
//ESPIPE        29      /* Illegal seek */
//EROFS         30      /* Read-only file system */
//EMLINK        31      /* Too many links */
//EPIPE         32      /* Broken pipe */
//ENOTEMPTY     36      /* Directory not empty */
//ENAMETOOLONG  40      /* The name given is too long */

//Use debugf and NOT printf() to make your
//debug outputs. Do not modify this function.
#if defined(DEBUG)
int debugf(const char *fmt, ...)
{
	int bytes = 0;
	va_list args;
	va_start(args, fmt);
	bytes = vfprintf(stderr, fmt, args);
	va_end(args);
	return bytes;
}
#else
int debugf(const char *fmt, ...)
{
	return 0;
}
#endif

typedef struct {
	BLOCK_HEADER drive_data;
	vector<NODE> list_of_nodes;
	vector<BLOCK> list_of_blocks;
	uint64_t nodes_in_use;
	uint64_t blocks_in_use;
} DRIVE, *PDRIVE;

//store all data about hard drive in DRIVE struct
DRIVE my_drive;
uint64_t bsize;

//debugging context 
void test() {
	for (uint64_t i = 0; i < my_drive.drive_data.nodes; i++) {
		//print out node id, name, and size 
		debugf("\n---Node %d---\nname: %s\nsize: %llu\n", my_drive.list_of_nodes[i].id, 
				my_drive.list_of_nodes[i].name, my_drive.list_of_nodes[i].size);
		//if node is for a file, print out block offsets
		if ((my_drive.list_of_nodes[i].mode & S_IFREG) == S_IFREG) {
			uint64_t number_of_blocks = my_drive.list_of_nodes[i].size / bsize + 1;
			debugf("block offsets: ");
			for (uint64_t j = 0; j < number_of_blocks; j++)
				debugf("%llu ", my_drive.list_of_nodes[i].blocks[j]);
			debugf("\n");
		}
	}
}

//returns amount of bytes used in the filesystem
uint64_t total_size() {
	return my_drive.blocks_in_use * bsize;
}

//finds node whose name is path. Returns pointer to that node if found, else returns NULL
PNODE find_node(const char *path) {
	PNODE ret = NULL;
	for (uint64_t i = 0; i < my_drive.drive_data.nodes; i++) {
		if (strcmp(my_drive.list_of_nodes[i].name, path) == 0) {
			ret = &my_drive.list_of_nodes[i];
			break;
		}
	}
	return ret;
}

//searches a NULL terminated array of strings for key. Returns true if found. Otherwise, returns false 
bool search_list(char **list, const char *key) {
	int i = 0;
	while (list[i] != NULL) {
		if (strcmp(list[i], key) == 0)
			return true;
		i++;
	}
	return false;
}

//creates empty block of data for node n
void create_block(PNODE n) {
	//update drive's number of blocks
	my_drive.drive_data.blocks += 1;
	my_drive.blocks_in_use += 1;
	uint64_t drive_blocks = my_drive.drive_data.blocks;
	//initialize block with empty data
	BLOCK new_block;
	new_block.data = (char *) malloc(bsize);
	memset(new_block.data, 0, bsize);
	//update node's block offsets
	uint64_t number_of_blocks = n->size / bsize + 1;
	void *resized_list = realloc(n->blocks, number_of_blocks * sizeof(uint64_t));
	n->blocks = (uint64_t *) resized_list;
	n->blocks[number_of_blocks - 1] = drive_blocks - 1;
	debugf("n->size = %llu. added block %llu at index %llu\n", n->size, drive_blocks - 1, number_of_blocks - 1);
	//update hard drive to include new block
	my_drive.list_of_blocks.push_back(new_block);
}

//free resources and make block data NULL
void delete_block(PBLOCK b) {
	my_drive.blocks_in_use -= 1;
	free(b->data);
	b->data = NULL;
}

int fs_drive(const char *dname)
{
	debugf("fs_drive: %s\n", dname);

	//open file with hard drive data and read in header
	FILE *hard_drive;
	BLOCK_HEADER drive_data;
	hard_drive = fopen(dname, "r");
	if (!hard_drive) 
		return -ENOENT;
	fread((char *)&drive_data, sizeof(BLOCK_HEADER), 1, hard_drive);
	//establish block size as global, so no need to reenter struct to get block size
	bsize = drive_data.block_size;

	//read in all node data and push onto respective array
	NODE node_data;
	uint64_t list;
	for (uint64_t i = 0; i < drive_data.nodes; i++) {
		fread((char *)&node_data, ONDISK_NODE_SIZE, 1, hard_drive);
		//if node is for a file, record block locations
		if ((node_data.mode & S_IFREG) == S_IFREG) {
			uint64_t number_of_blocks = node_data.size / bsize + 1;
			uint64_t *block_offsets = (uint64_t *) malloc(number_of_blocks * sizeof(uint64_t));
			for (uint64_t j = 0; j < number_of_blocks; j++) {
				fread((char *)&list, sizeof(list), 1, hard_drive);
				block_offsets[j] = list;
			}
			node_data.blocks = block_offsets;
		}
		my_drive.list_of_nodes.push_back(node_data);
	}

	//read in all block data and push onto respective array
	BLOCK block_data;
	char *data;
	for (uint64_t i = 0; i < drive_data.blocks; i++) {
		data = (char *) malloc(bsize);
		fread(data, bsize, 1, hard_drive);
		block_data.data = data;
		my_drive.list_of_blocks.push_back(block_data);
	}

	//read all data about hard drive into DRIVE struct
	my_drive.drive_data = drive_data;
	my_drive.nodes_in_use = drive_data.nodes;
	my_drive.blocks_in_use = drive_data.blocks;

	debugf("Magic: %s\nBlock Size: %lu\n# of Nodes: %lu\n# of Blocks: %lu\n", my_drive.drive_data.magic, 
			bsize, my_drive.drive_data.nodes, my_drive.drive_data.blocks);
	test();

	fclose(hard_drive);
	//check magic and return
	if (strcmp(drive_data.magic, "COSC_361") == 0)
		return 0;
	else
		return -EPERM;
}

int fs_open(const char *path, struct fuse_file_info *fi)
{
	debugf("fs_open: %s\n", path);
	//find if node for file exists
	PNODE my_node = find_node(path);
	if (my_node)
		return 0;
	else
		return -ENOENT; 
}

int fs_read(const char *path, char *buf, size_t size, off_t offset,
		struct fuse_file_info *fi)
{
	debugf("fs_read: %s\n", path);
	debugf("size = %d. offset = %d\n", size, offset);

	//find file given by path
	PNODE my_node = find_node(path);
	if (!my_node)
		return -ENOENT;
	debugf("file size being read is %llu\n", my_node->size);
	//check if reading out of bounds. If so, 0 bytes read
	if ((int) my_node->size < offset)
		return 0;

	//move to offset position
	uint64_t byte_offset = offset % bsize;
	uint64_t block_count = offset / bsize;
	uint64_t block_offset = my_node->blocks[block_count];
	//write data to buf
	uint64_t byte_count;
	for (byte_count = 0; byte_count < size; byte_count++) {
		if (my_drive.list_of_blocks[block_offset].data[byte_offset] == 0)
			break;
		buf[byte_count] = my_drive.list_of_blocks[block_offset].data[byte_offset];
		byte_offset++;
		//if reaches block data limit, go to next block
		if (byte_offset == bsize) {
			byte_offset = 0;
			block_count++;
			block_offset = my_node->blocks[block_count];
		}
	}
	//return number of bytes read
	return byte_count;
}

int fs_write(const char *path, const char *data, size_t size, off_t offset,
		struct fuse_file_info *fi)
{
	debugf("fs_write: %s\n", path);
	debugf("size = %d. offset = %d\n", size, offset);

	PNODE my_node = find_node(path);
	//node should be in DRIVE struct. This is a fail safe
	if (!my_node)
		return -ENOENT;
	//check if there is enough space to write
	if (total_size() + size > MAX_DRIVE_SIZE)
		return -ENOSPC;
	//check if read only file system
	if (fi->flags & O_RDONLY)
		return -EROFS;

	debugf("file size is %llu\n", my_node->size);
	uint64_t original_size = my_node->size;
	//preemptively create necessary blocks
	for (uint64_t number_of_blocks = my_node->size / bsize + 1;
		number_of_blocks < (size + offset) / bsize + 1; number_of_blocks++) {
		my_node->size += bsize;
		create_block(my_node);
	}

	uint64_t cur_block = offset / bsize + 1;
	uint64_t block_offset = my_node->blocks[cur_block - 1];
	BLOCK tmp = my_drive.list_of_blocks[block_offset];
	//determine which byte for cur_block to start writing data
	uint64_t byte_offset = offset % bsize;

	//begin writing data to blocks
	for (uint64_t i = 0; i < size; i++) {
		tmp.data[byte_offset] = data[i];
		byte_offset++;
		//if reach end of block, go to next block
		if (byte_offset == bsize) {
			cur_block++;
			block_offset = my_node->blocks[cur_block - 1];
			debugf("now writing to block %llu\n", block_offset);
			tmp = my_drive.list_of_blocks[block_offset];
			byte_offset = 0;
		}
	}
	//update size and return number of bytes written
	my_node->size = size + offset;
	if (original_size > my_node->size)
		my_node->size = original_size;
	return size;	
}

int fs_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
	debugf("fs_create: %s\n", path);

	//check if specified path is too long
	if (strlen(path) > NAME_SIZE)
		return -ENAMETOOLONG;
	//if node with path already specified, return error
	if (find_node(path))
		return -EEXIST;
	//check if using read only file system
	if (fi->flags & O_RDONLY)
		return -EROFS;

	//if node DNE, create new node and update superblock to reflect change
	my_drive.drive_data.nodes += 1;
	my_drive.nodes_in_use += 1;
	//initialize node
	NODE new_file;
	strcpy(new_file.name, path);
	new_file.id = my_drive.drive_data.nodes - 1;
	new_file.size = 0;
	new_file.uid = getuid();
	new_file.gid = getgid();
	new_file.mode = S_IFREG | mode;
	new_file.ctime = time(NULL);
	new_file.atime = time(NULL);
	new_file.mtime = time(NULL);	

	//initialize fresh block for node
	my_drive.drive_data.blocks += 1;
	my_drive.blocks_in_use += 1;
	uint64_t next_block = my_drive.drive_data.blocks;
	BLOCK new_block;
	new_block.data = (char *) malloc(bsize);
	memset(new_block.data, 0, bsize);
	//update node's block offsets
	new_file.blocks = (uint64_t *) malloc(sizeof(uint64_t));
	new_file.blocks[0] = next_block - 1;
	//update hard drive to include new block
	my_drive.list_of_blocks.push_back(new_block);
	//update hard drive to include new node
	my_drive.list_of_nodes.push_back(new_file);

	debugf("node: '%s' with id %llu and block %llu created\n", new_file.name, new_file.id, new_file.blocks[0]);
	return 0;
}

int fs_getattr(const char *path, struct stat *s) {
	debugf("fs_getattr: %s\n", path);

	//find node whose name is specified path
	PNODE my_node = find_node(path);

	if (!my_node)
		return -ENOENT;

	//store attributes
	s->st_dev = 0;
	s->st_rdev = 0;
	s->st_ino = my_node->id;
	s->st_mode = my_node->mode;
	s->st_nlink = 1;
	s->st_uid = my_node->uid;
	s->st_gid = my_node->gid;
	s->st_size = my_node->size;
	s->st_atime = my_node->atime;
	s->st_mtime = my_node->mtime;
	s->st_ctime = my_node->ctime;
	s->st_blksize = bsize;
	if ((my_node->mode & S_IFREG) == S_IFREG)
		s->st_blocks = (my_node->size / bsize + 1) * (bsize / 512);
	else
		s->st_blocks = 0;

	return 0;
}

int fs_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
		off_t offset, struct fuse_file_info *fi)
{
	debugf("fs_readdir: %s\n", path);

	//filler(buf, <name of file/directory>, 0, 0)
	filler(buf, ".", 0, 0);
	filler(buf, "..", 0, 0);

	//for all paths other than "/", make apparent that it is a subdir
	char parse_path[512];
	strcpy(parse_path, path);
	if (strcmp(parse_path, "/") != 0)
		strcat(parse_path, "/");

	//initialize list of unique names
	char *name_list[NAME_SIZE];
	int list_counter = 0;
	name_list[list_counter] = NULL;
	//go through each node and see if path prepends the node's name. If so, it means node is in the directory
	for (uint64_t i = 0; i < my_drive.drive_data.nodes; i++) {
		char *node_name = my_drive.list_of_nodes[i].name;
		char *ret = strstr(node_name, parse_path);

		if (ret == node_name) {
			//extract file/directory name within the '/' characters 
			int buf_counter = 0;
			char extract_buf[NAME_SIZE];
			for (size_t i = strlen(parse_path); i < strlen(node_name); i++) {
				if (node_name[i] == '/')
					break;
				extract_buf[buf_counter++] = node_name[i];
			}
			extract_buf[buf_counter] = '\0';

			//if name contains at least 1 character AND is not already in list, add it
			if (strlen(extract_buf) > 0 && !search_list(name_list, extract_buf)) {
				name_list[list_counter++] = strdup(extract_buf);
				name_list[list_counter] = NULL;
			}
		}
	}
	//output using filler()
	int i = 0;
	while (name_list[i] != NULL) {
		filler(buf, name_list[i], 0, 0);
		free(name_list[i++]);
	}
	return 0;
}

int fs_opendir(const char *path, struct fuse_file_info *fi)
{
	debugf("fs_opendir: %s\n", path);
	//find if node for directory exists
	PNODE my_node = find_node(path);
	if (my_node)
		return 0;
	else
		return -ENOENT; 
}

int fs_chmod(const char *path, mode_t mode)
{
	debugf("fs_chmod: %s\n", path);

	PNODE my_node = find_node(path);
	//return error if node with path DNE
	if (!my_node)
		return -ENOENT;
	//if found, change mode
	my_node->mode = mode;
	return 0;
}

int fs_chown(const char *path, uid_t uid, gid_t gid)
{
	debugf("fs_chown: %s\n", path);

	PNODE my_node = find_node(path);
	//return error if node with path DNE
	if (!my_node)
		return -ENOENT;
	//if found, change owndership
	my_node->uid = uid;
	my_node->gid = gid;
	return 0;
}

int fs_unlink(const char *path)
{
	debugf("fs_unlink: %s\n", path);

	PNODE my_node = find_node(path);
	//return error if node with path DNE or is a directory
	if (!my_node)
		return -ENOENT;
	else if ((my_node->mode & S_IFDIR) == S_IFDIR)
		return -EISDIR;
	//if path is a file, remove node and corresponding blocks
	debugf("deleting node with path %s\n", my_node->name);
	my_drive.nodes_in_use -= 1;
	strcpy(my_node->name, "");
	
	uint64_t del_block;
	for (uint64_t i = 0; i < my_node->size / bsize + 1; i++) {
		del_block = my_node->blocks[i];
		debugf("deleting block %llu\n", del_block);
		delete_block(&my_drive.list_of_blocks[del_block]);
	}

	return 0;
}

int fs_mkdir(const char *path, mode_t mode)
{
	debugf("fs_mkdir: %s\n", path);

	//check if specified path is too long
	if (strlen(path) > NAME_SIZE)
		return -ENAMETOOLONG;

	//if node with path already specified, return error
	if (find_node(path))
		return -EEXIST;

	//if node DNE, create new node and update superblock to reflect change
	my_drive.drive_data.nodes += 1;
	my_drive.nodes_in_use += 1;
	//initialize node
	NODE new_dir;
	strcpy(new_dir.name, path);
	new_dir.id = my_drive.drive_data.nodes - 1;
	new_dir.size = 0;
	new_dir.uid = getuid();
	new_dir.gid = getgid();
	new_dir.mode = S_IFDIR | mode;
	new_dir.ctime = time(NULL);
	new_dir.atime = time(NULL);
	new_dir.mtime = time(NULL);
	debugf("node with id %llu created\n", new_dir.id);

	//update hard drive to include new node
	my_drive.list_of_nodes.push_back(new_dir);
	return 0;
}

int fs_rmdir(const char *path)
{
	debugf("fs_rmdir: %s\n", path);

	PNODE my_node = find_node(path);
	//check if node DNE
	if (!my_node)
		return -ENOENT;
	//check if node is not a directory
	if ((my_node->mode & S_IFDIR) != S_IFDIR)
		return -ENOTDIR;
	//check if directory is not empty
	char parse_path[NAME_SIZE];
	strcpy(parse_path, path);
	strcat(parse_path, "/");
	for (uint64_t i = 0; i < my_drive.drive_data.nodes; i++) {
		if (strstr(my_drive.list_of_nodes[i].name, parse_path))
			return -ENOTEMPTY;
	}
	//if directory empty, clear node
	debugf("directory empty. deleting %s...\n", path);
	my_drive.nodes_in_use -= 1;
	strcpy(my_node->name, "");

	return 0;
}

int fs_rename(const char *path, const char *new_name)
{
	debugf("fs_rename: %s -> %s\n", path, new_name);

	PNODE my_node = find_node(path);
	//check if original path exists
	if (!my_node)
		return -ENOENT;
	//check if new path is used
	if (find_node(new_name))
		return -EEXIST;
	//check if new path is valid path
	char parse_path[NAME_SIZE];
	strcpy(parse_path, new_name);
	//extract subdirectory path to new name by concatenating at most right /
	for (size_t i = strlen(new_name) - 1; i >= 0; i--) {
		if (parse_path[i] == '/') {
			parse_path[i] = '\0';
			if (strcmp(parse_path, "") == 0)
				strcpy(parse_path, "/");
			break;
		}
	}
	debugf("extracted %s\n", parse_path);
	//go through each node and see if extracted path is one's substring
	for (uint64_t i = 0; i < my_drive.drive_data.nodes; i++) {
		NODE tmp = my_drive.list_of_nodes[i];
		char *ret = strstr(tmp.name, parse_path);
		if (ret == tmp.name) {
			debugf("path is valid. ret = %s\n", ret);
			//if valid path, go ahead and change name
			strcpy(my_node->name, new_name);
			
			test();
			return 0;
		}
	}
	//if new path DNE, return error
	return -ENOENT;
}

int fs_truncate(const char *path, off_t size)
{
	debugf("fs_truncate: %s to size %d\n", path, size);

	PNODE my_node = find_node(path);
	//check if node DNE
	if (!my_node)
		return -ENOENT;
	//determine how many blocks and respective bytes to use
	uint64_t blocks_to_use = size / bsize + 1;
	uint64_t bytes_to_use = size % bsize;
	//number of blocks the node currently uses
	uint64_t blocks_of_node = my_node->size / bsize + 1;
	//truncate blocks
	for (uint64_t i = 0; i < blocks_of_node - blocks_to_use; i++) {
		uint64_t block_offset = my_node->blocks[blocks_of_node - 1 - i];
		debugf("deleting block %llu\n", block_offset);
		delete_block(&my_drive.list_of_blocks[block_offset]);
	}
	//truncate bytes
	for (uint64_t i = bytes_to_use; i < bsize; i++) {
		uint64_t block_offset = my_node->blocks[blocks_to_use -1];
		my_drive.list_of_blocks[block_offset].data[i] = 0;
	}
	//update size of node
	my_node->size = size;

	return 0;
}

void fs_destroy(void *ptr)
{
	const char *filename = (const char *)ptr;
	debugf("fs_destroy: %s\n", filename);

	//open hard_drive for writing
	FILE *hard_drive;
	hard_drive = fopen(filename, "w");

	//save total number of nodes (used and unused)
	uint64_t node_total = my_drive.drive_data.nodes;

	//update block header data
	my_drive.drive_data.nodes = my_drive.nodes_in_use;
	my_drive.drive_data.blocks = my_drive.blocks_in_use;
	//write block header to file
	fwrite(&my_drive.drive_data, sizeof(BLOCK_HEADER), 1, hard_drive);

	uint64_t node_count = 0;
	uint64_t block_count = 0;
	//write node data, ignoring unused nodes
	for (uint64_t i = 0; i < node_total; i++) {
		NODE my_node = my_drive.list_of_nodes[i];
		if (strcmp(my_node.name, "") != 0) {
			//reset id counter and push onto temporary array
			my_node.id = node_count;
			fwrite(&my_node, ONDISK_NODE_SIZE, 1, hard_drive);
			debugf("%s has id %llu\n", my_node.name, my_node.id);
			//if node is file, write block offsets
			if ((my_node.mode & S_IFREG) == S_IFREG) {
				uint64_t number_of_blocks = my_node.size / bsize + 1;
				for (uint64_t j = 0; j < number_of_blocks; j++) {
					fwrite(&block_count, sizeof(uint64_t), 1, hard_drive);
					debugf("%llu\n", block_count);
					block_count++;
				}
			}
			node_count++;
		}
	}

	//write block data by going through reordered node list each node's blocks
	char *data = (char *) malloc(bsize);
	for (uint64_t i = 0; i < node_total; i++) {
		NODE my_node = my_drive.list_of_nodes[i];
		//if node is file, write blocks and change block offsets to new positions
		if (strcmp(my_node.name, "") != 0 && (my_node.mode & S_IFREG) == S_IFREG) {
			uint64_t number_of_blocks = my_node.size / bsize + 1;
			for (uint64_t j = 0; j < number_of_blocks; j++) {
				//get original block offset and respective block
				uint64_t block_offset = my_node.blocks[j];
				BLOCK my_block = my_drive.list_of_blocks[block_offset];
				//if block is in use, write to hard drive
				if (my_block.data) {
					memset(data, 0, bsize);
					memcpy(data, my_block.data, bsize);
					fwrite(data, bsize, 1, hard_drive);
					free(my_block.data);
				}
			}
			free(my_node.blocks);
		}
	}
	fclose(hard_drive);
	free(data);
}

//////////////////////////////////////////////////////////////////
//int main()
//DO NOT MODIFY THIS FUNCTION
//////////////////////////////////////////////////////////////////
int main(int argc, char *argv[])
{
	fuse_operations *fops;
	char *evars[] = { "./fs", "-f", "mnt", NULL };
	int ret;

	if ((ret = fs_drive(HARD_DRIVE)) != 0) {
		debugf("Error reading hard drive: %s\n", strerror(-ret));
		return ret;
	}
	//FUSE operations
	fops = (struct fuse_operations *) calloc(1, sizeof(struct fuse_operations));
	fops->getattr = fs_getattr;
	fops->readdir = fs_readdir;
	fops->opendir = fs_opendir;
	fops->open = fs_open;
	fops->read = fs_read;
	fops->write = fs_write;
	fops->create = fs_create;
	fops->chmod = fs_chmod;
	fops->chown = fs_chown;
	fops->unlink = fs_unlink;
	fops->mkdir = fs_mkdir;
	fops->rmdir = fs_rmdir;
	fops->rename = fs_rename;
	fops->truncate = fs_truncate;
	fops->destroy = fs_destroy;

	debugf("Press CONTROL-C to quit\n\n");

	return fuse_main(sizeof(evars) / sizeof(evars[0]) - 1, evars, fops,
			(void *)HARD_DRIVE);
}
