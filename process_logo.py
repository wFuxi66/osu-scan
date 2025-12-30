from PIL import Image
import os

def remove_background(img):
    img = img.convert("RGBA")
    datas = img.getdata()
    
    new_data = []
    # Threshold for "Black" background (the generated image has a dark grey/black bg)
    threshold = 40 
    
    for item in datas:
        # Check if pixel is dark (R, G, B are all low)
        if item[0] < threshold and item[1] < threshold and item[2] < threshold:
            # Make it transparent
            new_data.append((255, 255, 255, 0))
        else:
            new_data.append(item)
            
    img.putdata(new_data)
    return img

try:
    # 1. Open original logo (we should reload from source if possible to avoid re-processing, 
    # but re-processing the black one is fine)
    # Actually, let's use the one in static/logo.png which is currently the full image with black bg.
    img = Image.open("static/logo.png")
    
    # 2. Make Transparent
    print("Removing background...")
    img_transparent = remove_background(img)
    img_transparent.save("static/logo.png", "PNG")
    print("Saved transparent logo.png")
    
    # 3. Create Favicon (Crop Only the Icon)
    # layout: Icon is Top-Centered. Text is Bottom.
    # Image size is likely 1024x1024.
    width, height = img_transparent.size
    
    # Crop a square from the top center.
    # Let's say the icon takes up the top 65% of the image.
    crop_size = int(height * 0.65)
    
    left = (width - crop_size) // 2
    top = int(height * 0.05) # Slight offset from top
    right = left + crop_size
    bottom = top + crop_size
    
    favicon = img_transparent.crop((left, top, right, bottom))
    
    # Resize to standard favicon size (e.g. 128x128) for sharpness
    favicon = favicon.resize((128, 128), Image.Resampling.LANCZOS)
    
    favicon.save("static/favicon.png", "PNG")
    print("Saved transparent favicon.png (Icon only)")
    
except ImportError:
    print("PIL not installed.")
except Exception as e:
    print(f"Error: {e}")
