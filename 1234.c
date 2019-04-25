#include <stdio.h>

int main()
{
	unsigned char arr[]={0x00,0x05,0xFE,0xFF,0xA5};
	int loop;
	
	printf("Array elements are:\n");
	for(loop=0;loop<5;loop++)
		printf("%02x ",arr[loop]);
	
	return 0;	
}
