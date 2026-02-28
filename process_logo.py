from PIL import Image
import os

def process_logo():
    try:
        print("Processing logo...")
        img = Image.open("static/logo.png")
        img = img.convert("RGBA")
        
        datas = img.getdata()
        new_data = []
        
        # Aggressive threshold for dark background noise
        # This will separate the pink/white logo from the dark grey background
        threshold = 80 
        
        for item in datas:
            # item is (R, G, B, A)
            if item[0] < threshold and item[1] < threshold and item[2] < threshold:
                # Transparent
                new_data.append((255, 255, 255, 0))
            else:
                new_data.append(item)
                
        img.putdata(new_data)
        
        # Now find the bounding box of non-transparent pixels
        bbox = img.getbbox()
        if bbox:
            print(f"Original size: {img.size}")
            print(f"Cropping to content: {bbox}")
            img = img.crop(bbox)
            print(f"New size: {img.size}")
        
        # Save the clean, cropped main logo
        img.save("static/logo.png", "PNG")
        print("Saved cleaned static/logo.png")
        
        # --- Make Favicon ---
        # The logo is likely vertical (Icon on top, Text on bottom) or just the icon if cropped tight.
        # Assuming the text is at the bottom, the icon is the top part.
        width, height = img.size
        
        # Let's take the largest square possible from the TOP center.
        # If the image is wider than tall (horizontal logo), take center square.
        # If taller than wide (vertical logo), take top square.
        
        size = min(width, height)
        # However, if it includes text at bottom, we want to exclude it.
        # Let's guess the icon is the top 70-80% of the image height.
        
        # Heuristic: Cut off bottom 20% (text) and take square from top?
        # Or just take a square from the top-center.
        
        crop_size = min(width, int(height * 0.8)) # Assume text is bottom 20%
        # Make it square
        
        left = (width - crop_size) // 2
        top = 0
        right = left + crop_size
        bottom = crop_size
        
        favicon = img.crop((left, top, right, bottom))
        favicon = favicon.resize((128, 128), Image.Resampling.LANCZOS)
        favicon.save("static/favicon.png", "PNG")
        print("Saved static/favicon.png")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == '__main__':
    process_logo()
